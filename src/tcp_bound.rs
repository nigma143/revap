use std::{
    collections::HashMap,
    error::Error,
    io::{self},
    net::SocketAddr,
    sync::Arc,
};

use tokio::{
    io::{split, AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
};

use tokio_rustls::rustls::{self};
use tokio_rustls::TlsAcceptor;

use crate::{
    balancing::{LoadBalancing, WildMatchGroup},
    bound::{io_process, Incoming, Outbound},
    domain::{BalanceType, InboundInfo, OutboundInfo},
};

pub struct TcpInbound {
    info: Arc<InboundInfo>,
    listener: TcpListener,
    tls_acceptor: Option<TlsAcceptor>,
    sni_map: Option<HashMap<String, Vec<String>>>,
}

impl TcpInbound {
    pub async fn bind_tcp(
        info: Arc<InboundInfo>,
        addr: SocketAddr,
    ) -> Result<Self, Box<dyn Error>> {
        TcpInbound::bind(info, addr, None, None).await
    }

    pub async fn bind_tls(
        info: Arc<InboundInfo>,
        addr: SocketAddr,
        sni_map: Option<HashMap<String, Vec<String>>>,
        cert_chain: Vec<rustls::Certificate>,
        key_der: rustls::PrivateKey,
    ) -> Result<Self, Box<dyn Error>> {
        let config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key_der)?;
        TcpInbound::bind(
            info,
            addr,
            sni_map,
            Some(TlsAcceptor::from(Arc::new(config))),
        )
        .await
    }

    async fn bind(
        info: Arc<InboundInfo>,
        addr: SocketAddr,
        sni_map: Option<HashMap<String, Vec<String>>>,
        tls_acceptor: Option<TlsAcceptor>,
    ) -> Result<Self, Box<dyn Error>> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            info,
            listener,
            tls_acceptor,
            sni_map,
        })
    }

    pub fn info(&self) -> &Arc<InboundInfo> {
        &self.info
    }

    pub async fn forwarding(&mut self, mut outbounds: Vec<Outbound>) {
        log::info!(
            "forwarding `{}` => `{}` ({:?})",
            self.info().alias(),
            outbounds
                .iter()
                .map(|x| x.info().alias())
                .collect::<Vec<&str>>()
                .join(", "),
            self.info().balance_type(),
        );
        let sni_outbounds = {
            match self.sni_map {
                Some(ref map) => {
                    let mut res = vec![];
                    for (sni, vals) in map {
                        let outbounds: Vec<Outbound> = outbounds
                            .drain_filter(|x| vals.iter().any(|y| y == x.info().alias()))
                            .collect();
                        let outbounds = match self.info().balance_type() {
                            BalanceType::RoundRobin => LoadBalancing::round_robin(outbounds),
                            BalanceType::LeastConn => LoadBalancing::least_conn(outbounds),
                        };
                        res.push((sni.as_str(), outbounds));
                    }
                    Arc::new(Some(WildMatchGroup::new(res)))
                }
                None => Arc::new(None),
            }
        };
        let outbounds = match self.info().balance_type() {
            BalanceType::RoundRobin => Arc::new(LoadBalancing::round_robin(outbounds)),
            BalanceType::LeastConn => Arc::new(LoadBalancing::least_conn(outbounds)),
        };
        while let Ok((stream, rem_addr)) = self.listener.accept().await {
            let info = self.info.clone();
            info.stats().active_conn_inc();
            {
                let tls_acceptor = self.tls_acceptor.clone();
                let sni_outbounds = sni_outbounds.clone();
                let outbounds = outbounds.clone();
                tokio::spawn(async move {
                    match tls_acceptor {
                        Some(acceptor) => match acceptor.accept(stream).await {
                            Ok(mut stream) => {
                                let (_, conn) = stream.get_ref();
                                let mut outbound = conn
                                    .sni_hostname()
                                    .and_then(|x| {
                                        (*sni_outbounds).as_ref().and_then(|y| y.select(x))
                                    })
                                    .map_or_else(|| outbounds.select(), |x| x);
                                let res =
                                    TcpInbound::s_forwarding(&mut stream, &mut outbound).await;
                                if let Err(e) = res {
                                    let (_, conn) = stream.get_ref();
                                    log::debug!(
                                            "forwarding error `{}` => `{}` (client_addr: {}, sni: {:?}). {}",
                                            info.alias(),
                                            outbound.info().alias(),
                                            rem_addr,
                                            conn.sni_hostname(),
                                            e
                                        );
                                }
                            }
                            Err(e) => {
                                log::debug!(
                                    "accept tls error at `{}` (client_addr: {}). {}",
                                    info.alias(),
                                    rem_addr,
                                    e
                                );
                            }
                        },
                        None => {
                            let mut outbound = outbounds.select();
                            let res = TcpInbound::s_forwarding(stream, &mut outbound).await;
                            if let Err(e) = res {
                                log::debug!(
                                    "forwarding error `{}` => `{}` (client_addr: {}). {}",
                                    info.alias(),
                                    outbound.info().alias(),
                                    rem_addr,
                                    e
                                );
                            }
                        }
                    }
                    info.stats().active_conn_dec();
                });
            }
        }
    }

    async fn s_forwarding<S: AsyncRead + AsyncWrite>(
        stream: S,
        outbound: &mut Outbound,
    ) -> io::Result<()> {
        let (reader, writer) = split(stream);
        outbound.forward(Incoming { reader, writer }).await
    }
}

#[derive(Clone)]
pub struct TcpOutbound {
    info: Arc<OutboundInfo>,
    addr: SocketAddr,
    tls_connector: Option<tokio_rustls::TlsConnector>,
}

struct VerifierDummy;

impl rustls::client::ServerCertVerifier for VerifierDummy {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

impl TcpOutbound {
    pub fn new_tcp(info: Arc<OutboundInfo>, addr: SocketAddr) -> Self {
        TcpOutbound::new(info, addr, None)
    }

    pub fn new_tls(info: Arc<OutboundInfo>, addr: SocketAddr) -> Self {
        let config = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(Arc::new(VerifierDummy {}))
            .with_no_client_auth();
        let tls_connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        TcpOutbound::new(info, addr, Some(tls_connector))
    }

    fn new(
        info: Arc<OutboundInfo>,
        addr: SocketAddr,
        tls_connector: Option<tokio_rustls::TlsConnector>,
    ) -> Self {
        Self {
            info,
            addr,
            tls_connector,
        }
    }

    pub fn info(&self) -> &Arc<OutboundInfo> {
        &self.info
    }

    pub async fn forward<R, W>(&mut self, incoming: Incoming<R, W>) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
        R: AsyncRead + Unpin,
    {
        self.info().stats().active_conn_inc();
        let res = {
            let stream = TcpStream::connect(self.addr).await?;
            if let Some(tls_connector) = self.tls_connector.as_mut() {
                let stream = tls_connector
                    .connect("domain".try_into().unwrap(), stream)
                    .await?;
                TcpOutbound::s_forward(incoming, stream).await
            } else {
                TcpOutbound::s_forward(incoming, stream).await
            }
        };
        self.info().stats().active_conn_dec();
        res
    }

    async fn s_forward<R, W, S>(incoming: Incoming<R, W>, stream: S) -> io::Result<()>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
        S: AsyncRead + AsyncWrite,
    {
        let Incoming {
            reader: ri,
            writer: wi,
        } = incoming;

        let (ro, wo) = split(stream);
        io_process(ri, wi, ro, wo).await
    }
}
