use std::{
    error::Error,
    fmt::{self, format},
    io::{self, BufReader},
    net::SocketAddr,
    path::Path,
    sync::Arc,
};

use log::{debug, error, info};
use rustls::ClientConfig;
use rustls_pemfile::{certs, rsa_private_keys};
use tokio::{
    io::{split, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::{self, Certificate, PrivateKey};
use tokio_rustls::TlsAcceptor;

use crate::{
    main,
    outbound::{self, io_process, Incoming, Outbound},
    revtcp_bound::RevTcpOutbound,
};

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
        end_entity: &rustls::Certificate,
        intermediates: &[rustls::Certificate],
        server_name: &rustls::ServerName,
        scts: &mut dyn Iterator<Item = &[u8]>,
        ocsp_response: &[u8],
        now: std::time::SystemTime,
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

    pub async fn forwarding<R, W>(&mut self, incoming: Incoming<R, W>) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
        R: AsyncRead + Unpin,
    {
        let (source, ri, wi) = match incoming {
            Incoming::Stream {
                source,
                reader,
                writer,
            } => (source, reader, writer),
        };

        match TcpStream::connect(self.addr).await {
            Ok(stream) => match self.tls_connector.as_mut() {
                Some(tls_connector) => {
                    match tls_connector
                        .connect("domain".try_into().unwrap(), stream)
                        .await
                    {
                        Ok(stream) => {
                            let (ro, wo) = split(stream);
                            match io_process(ri, wi, ro, wo).await {
                                Ok(_) => {}
                                Err(e) => error!(
                                    "error at process {} --> TLS({}). detail: {}",
                                    source, self.addr, e
                                ),
                            }
                        }
                        Err(e) => todo!(),
                    }
                }
                None => {
                    let (ro, wo) = split(stream);
                    match io_process(ri, wi, ro, wo).await {
                        Ok(_) => {}
                        Err(e) => error!(
                            "error at process {} --> TLS({}). detail: {}",
                            source, self.addr, e
                        ),
                    }
                }
            },
            Err(_) => todo!(),
        }
        Ok(())
    }
}

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

    pub async fn forwarding(&mut self, mut outbounds: Vec<Outbound>) -> io::Result<()> {
        let mut index = 0;
        while let Ok((stream, rem_addr)) = self.listener.accept().await {
            let alias = self.alias.clone();
            let tls_acceptor = self.tls_acceptor.clone();

            if index >= outbounds.len() {
                index = 0;
            }
            let mut outbound = outbounds.get_mut(index).unwrap().clone();
            index += 1;

            tokio::spawn(async move {
                let res = match tls_acceptor {
                    Some(tls_acceptor) => match tls_acceptor.accept(stream).await {
                        Ok(stream) => forwarding(stream, &mut outbound).await,
                        Err(e) => Err(e),
                    },
                    None => forwarding(stream, &mut outbound).await,
                };

                if let Err(e) = res {
                    log::error!(
                        "error on process ({}) -> ({}). details: {}",
                        alias,
                        outbound.alias(),
                        e
                    );
                }
            });
        }
        Ok(())
    }
}

async fn forwarding<S: AsyncRead + AsyncWrite>(
    stream: S,
    outbound: &mut Outbound,
) -> io::Result<()> {
    let (reader, writer) = split(stream);
    outbound
        .forwarding(Incoming::Stream {
            source: "sdfds".into(),
            reader,
            writer,
        })
        .await
}
