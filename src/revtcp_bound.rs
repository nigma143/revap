use std::{
    error::Error,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{channel, Sender},
        oneshot,
    },
    time,
};
use tokio_rustls::TlsAcceptor;

use crate::{
    balancing::LoadBalancing,
    bound::{io_process, Incoming, Outbound},
    domain::{BalanceType, InboundInfo, OutboundInfo},
    mux::{ChannelId, MuxConnection, MuxConnector},
    pipe::{PipeReader, PipeWriter},
};

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

pub struct RevTcpInbound {
    info: Arc<InboundInfo>,
    addr: SocketAddr,
    access_key: String,
    tls_connector: Option<tokio_rustls::TlsConnector>,
    mux: Option<MuxConnection>,
}

impl RevTcpInbound {
    pub fn bind_tcp(
        info: Arc<InboundInfo>,
        addr: SocketAddr,
        access_key: impl Into<String>,
    ) -> Self {
        RevTcpInbound::bind(info, addr, access_key.into(), None)
    }

    pub fn bind_tls(
        info: Arc<InboundInfo>,
        addr: SocketAddr,
        access_key: impl Into<String>,
    ) -> Self {
        let config = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(Arc::new(VerifierDummy {}))
            .with_no_client_auth();
        let tls_connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        RevTcpInbound::bind(info, addr, access_key.into(), Some(tls_connector))
    }

    fn bind(
        info: Arc<InboundInfo>,
        addr: SocketAddr,
        access_key: String,
        tls_connector: Option<tokio_rustls::TlsConnector>,
    ) -> Self {
        Self {
            info,
            addr,
            access_key,
            tls_connector,
            mux: None,
        }
    }

    pub fn info(&self) -> &Arc<InboundInfo> {
        &self.info
    }

    pub async fn forwarding(&mut self, outbounds: Vec<Outbound>) {
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
        let mut outbounds = match self.info().balance_type() {
            BalanceType::RoundRobin => LoadBalancing::round_robin(outbounds),
            BalanceType::LeastConn => LoadBalancing::least_conn(outbounds),
        };
        while let Ok((id, reader, writer)) = self.accept().await {
            let info = self.info().clone();
            info.stats().active_conn_inc();
            {
                let mut outbound = outbounds.select().clone();
                tokio::spawn(async move {
                    let res = outbound.forward(Incoming { reader, writer }).await;
                    if let Err(e) = res {
                        log::debug!(
                            "forwarding `{}` => `{}` (channel_id: {}). {}",
                            info.alias(),
                            outbound.info().alias(),
                            id,
                            e
                        );
                    }
                    info.stats().active_conn_dec();
                });
            }
        }
    }

    async fn accept(&mut self) -> io::Result<(ChannelId, PipeReader, PipeWriter)> {
        loop {
            match self.mux.as_mut() {
                Some(mux) => {
                    match mux.accept().await {
                        Ok(channel) => {
                            return Ok(channel);
                        }
                        Err(e) => {
                            log::error!("listen mux channel from {}. {}", self.addr, e);
                            self.mux = None;
                        }
                    };
                }
                None => {
                    let res = match TcpStream::connect(self.addr).await {
                        Ok(stream) => match self.tls_connector.as_mut() {
                            Some(tls_connector) => {
                                match tls_connector
                                    .connect("domain".try_into().unwrap(), stream)
                                    .await
                                {
                                    Ok(stream) => create_client_mux(stream, &self.access_key).await,
                                    Err(e) => Err(format!("bad tls. {}", e).into()),
                                }
                            }
                            None => create_client_mux(stream, &self.access_key).await,
                        },
                        Err(e) => Err(e.into()),
                    };

                    match res {
                        Ok(mux) => {
                            log::info!("connected to mux-server {}", self.addr);
                            self.mux = Some(mux);
                        }
                        Err(e) => {
                            log::error!("connecting to mux-server {}. {}", self.addr, e);
                            time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct RevTcpOutbound {
    info: Arc<OutboundInfo>,
    conn_tx: Sender<oneshot::Sender<Option<MuxConnector>>>,
}

impl RevTcpOutbound {
    pub async fn bind_tcp(
        info: Arc<OutboundInfo>,
        addr: SocketAddr,
        access_keys: Vec<String>,
    ) -> Result<Self, Box<dyn Error>> {
        RevTcpOutbound::bind(info, addr, access_keys, None).await
    }

    pub async fn bind_tls(
        info: Arc<OutboundInfo>,
        addr: SocketAddr,
        access_keys: Vec<String>,
        cert_chain: Vec<rustls::Certificate>,
        key_der: rustls::PrivateKey,
    ) -> Result<Self, Box<dyn Error>> {
        let config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key_der)?;
        RevTcpOutbound::bind(
            info,
            addr,
            access_keys,
            Some(TlsAcceptor::from(Arc::new(config))),
        )
        .await
    }

    async fn bind(
        info: Arc<OutboundInfo>,
        addr: SocketAddr,
        access_keys: Vec<String>,
        tls_acceptor: Option<TlsAcceptor>,
    ) -> Result<Self, Box<dyn Error>> {
        let pool: Arc<Mutex<Vec<MuxConnection>>> = Arc::new(Mutex::new(Vec::new()));
        let pool_ref = pool.clone();
        let (conn_tx, mut conn_rx) = channel(1024);

        tokio::spawn(async move {
            let mut index = 0;
            loop {
                match conn_rx.recv().await {
                    Some(rq) => {
                        let rq: oneshot::Sender<Option<MuxConnector>> = rq;
                        let mut pool = pool_ref.lock().unwrap();
                        pool.retain(|mux| !mux.is_closed());
                        if pool.is_empty() {
                            let _ = rq.send(None);
                            continue;
                        }
                        if pool.len() - 1 < index {
                            index = 0;
                        }
                        let mux = pool.get_mut(index).unwrap();
                        let _ = rq.send(Some(mux.create_connector()));
                        index += 1;
                    }
                    None => break,
                }
            }
        });

        let listener = TcpListener::bind(addr).await?;
        let access_keys = Arc::new(Mutex::new(access_keys));

        tokio::spawn(async move {
            while let Ok((stream, rem_addr)) = listener.accept().await {
                let tls_acceptor = tls_acceptor.clone();
                let access_keys = access_keys.clone();
                let pool = pool.clone();
                tokio::spawn(async move {
                    let res = match tls_acceptor {
                        Some(tls_acceptor) => match tls_acceptor.accept(stream).await {
                            Ok(stream) => create_server_mux(stream, access_keys).await,
                            Err(e) => Err(format!("bad tls. {}", e).into()),
                        },
                        None => create_server_mux(stream, access_keys).await,
                    };
                    match res {
                        Ok(mux) => {
                            log::info!("connected mux-agent on {} from {}", addr, rem_addr);
                            pool.lock().unwrap().push(mux);
                        }
                        Err(e) => {
                            log::error!("accepting mux-agent on {} from {}. {}", addr, rem_addr, e);
                        }
                    }
                });
            }
        });

        Ok(Self { info, conn_tx })
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
            let Incoming {
                reader: ri,
                writer: wi,
            } = incoming;

            let (tx, rx) = oneshot::channel();
            let _ = self.conn_tx.send(tx).await;
            let mut mux = rx.await.unwrap().ok_or(io::Error::new(
                io::ErrorKind::Other,
                "transport connection is not established",
            ))?;
            let (_id, ro, wo) = mux.connect().await?;
            io_process(ri, wi, ro, wo).await
        };
        self.info().stats().active_conn_dec();
        res
    }
}

const ACCESS_GRANTED: u8 = 0x00;
const INVALID_ACCESS_KEY: u8 = 0xFF;

async fn create_client_mux<T: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    mut stream: T,
    access_key: &str,
) -> Result<MuxConnection, Box<dyn Error + Send + Sync>> {
    obtain_access(&mut stream, access_key).await?;
    Ok(MuxConnection::new(stream))
}

async fn obtain_access<T: AsyncRead + AsyncWrite + Unpin>(
    mut stream: T,
    access_key: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let buf = access_key.as_bytes();
    stream.write_u8(buf.len() as u8).await?;
    stream.write_all(buf).await?;
    match stream.read_u8().await? {
        ACCESS_GRANTED => Ok(()),
        INVALID_ACCESS_KEY => Err("invalid access key".into()),
        _ => Err("unexpected access result code".into()),
    }
}

async fn create_server_mux<T: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    mut stream: T,
    access_keys: Arc<Mutex<Vec<String>>>,
) -> Result<MuxConnection, Box<dyn Error>> {
    check_access(&mut stream, access_keys).await?;
    Ok(MuxConnection::new(stream))
}

async fn check_access<T: AsyncRead + AsyncWrite + Unpin>(
    mut stream: T,
    access_keys: Arc<Mutex<Vec<String>>>,
) -> Result<(), Box<dyn Error>> {
    let fut = async {
        let len = stream.read_u8().await? as usize;
        let mut buf = Vec::with_capacity(len);
        unsafe {
            buf.set_len(len);
        }
        stream.read_exact(&mut buf).await?;
        let access_key = String::from_utf8(buf)?;
        let result = access_keys.lock().unwrap().contains(&access_key);
        if result {
            stream.write_u8(ACCESS_GRANTED).await?;
            Result::<(), Box<dyn Error>>::Ok(())
        } else {
            stream.write_u8(INVALID_ACCESS_KEY).await?;
            Err("invalid access key".into())
        }
    };

    if let Result::<Result<(), Box<dyn Error>>, time::error::Elapsed>::Ok(res) =
        time::timeout(Duration::from_secs(1), fut).await
    {
        res
    } else {
        Err("timeout at check access key".into())
    }
}
