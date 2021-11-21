use std::{
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
use tracing::{Instrument, debug, error, info_span};

use crate::bound::{io_process, Incoming, Outbound};

pub struct TcpInbound {
    alias: String,
    listener: TcpListener,
    tls_acceptor: Option<TlsAcceptor>,
}

impl TcpInbound {
    pub async fn bind_tcp(
        alias: impl Into<String>,
        addr: SocketAddr,
    ) -> Result<Self, Box<dyn Error>> {
        TcpInbound::bind(alias.into(), addr, None).await
    }

    pub async fn bind_tls(
        alias: impl Into<String>,
        addr: SocketAddr,
        cert_chain: Vec<rustls::Certificate>,
        key_der: rustls::PrivateKey,
    ) -> Result<Self, Box<dyn Error>> {
        let config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key_der)?;
        TcpInbound::bind(
            alias.into(),
            addr,
            Some(TlsAcceptor::from(Arc::new(config))),
        )
        .await
    }

    async fn bind(
        alias: String,
        addr: SocketAddr,
        tls_acceptor: Option<TlsAcceptor>,
    ) -> Result<Self, Box<dyn Error>> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            alias,
            listener,
            tls_acceptor,
        })
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }

    pub async fn forwarding(&mut self, outbounds: &mut Vec<Outbound>) -> io::Result<()> {
        let mut index = 0;
        while let Ok((stream, rem_addr)) = self.listener.accept().await {
            let alias = self.alias.clone();
            let tls_acceptor = self.tls_acceptor.clone();

            if index >= outbounds.len() {
                index = 0;
            }
            let mut outbound = outbounds.get_mut(index).unwrap().clone();
            index += 1;

            let span = info_span!(
                "forwarding",
                client = format!("{}", rem_addr).as_str(),
                target = outbound.alias()
            );
            tokio::spawn(async move {
                debug!("started");
                let res = {
                    if let Some(tls_acceptor) = tls_acceptor {
                        match tls_acceptor.accept(stream).await {
                            Ok(stream) => TcpInbound::s_forwarding(stream, &mut outbound).await,
                            Err(e) => Err(e),
                        }
                    } else {
                        TcpInbound::s_forwarding(stream, &mut outbound).await
                    }
                };
                if let Err(e) = res {
                    error!("{}", e);
                }
                debug!("end");
            }.instrument(span));
        }
        Ok(())
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
    alias: String,
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
    pub fn new_tcp(alias: impl Into<String>, addr: SocketAddr) -> Self {
        TcpOutbound::new(alias.into(), addr, None)
    }

    pub fn new_tls(alias: impl Into<String>, addr: SocketAddr) -> Self {
        let config = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(Arc::new(VerifierDummy {}))
            .with_no_client_auth();
        let tls_connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        TcpOutbound::new(alias.into(), addr, Some(tls_connector))
    }

    fn new(
        alias: String,
        addr: SocketAddr,
        tls_connector: Option<tokio_rustls::TlsConnector>,
    ) -> Self {
        Self {
            alias,
            addr,
            tls_connector,
        }
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }

    pub async fn forward<R, W>(&mut self, incoming: Incoming<R, W>) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
        R: AsyncRead + Unpin,
    {
        let stream = TcpStream::connect(self.addr).await?;
        if let Some(tls_connector) = self.tls_connector.as_mut() {
            let stream = tls_connector
                .connect("domain".try_into().unwrap(), stream)
                .await?;
            TcpOutbound::s_forward(incoming, stream).await
        } else {
            TcpOutbound::s_forward(incoming, stream).await
        }
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
