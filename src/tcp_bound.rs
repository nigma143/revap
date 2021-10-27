use std::{
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
    outbound::{io_process, Incoming, Outbound},
    revtcp_bound::RevTcpOutbound,
};

#[derive(Clone)]
pub struct TcpOutbound {
    pub addr: SocketAddr,
}

impl TcpOutbound {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    pub async fn connect(&mut self) -> io::Result<(ReadHalf<TcpStream>, WriteHalf<TcpStream>)> {
        let stream = TcpStream::connect(self.addr).await?;
        Ok(split(stream))
    }

    pub async fn forwarding<R, W>(&mut self, incoming: Incoming<R, W>)
    where
        W: AsyncWrite + Unpin,
        R: AsyncRead + Unpin,
    {
        let (source, ir, iw) = match incoming {
            Incoming::Stream {
                source,
                reader,
                writer,
            } => (source, reader, writer),
        };

        match self.connect().await {
            Ok((or, ow)) => {
                debug!("forwarding {} --> TLS({})", source, self.addr);
                match io_process(ir, iw, or, ow).await {
                    Ok(_) => {}
                    Err(e) => error!(
                        "error at process {} --> TLS({}). detail: {}",
                        source, self.addr, e
                    ),
                }
            }
            Err(_) => todo!(),
        }
    }
}

#[derive(Clone)]
pub struct TlsOutbound {
    pub addr: SocketAddr,
    config: Arc<ClientConfig>,
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

impl TlsOutbound {
    pub fn new(addr: SocketAddr) -> Self {
        let mut config = ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(Arc::new(VerifierDummy {}))
            .with_no_client_auth();
        //config.enable_sni = false;

        Self {
            addr,
            config: Arc::new(config),
        }
    }

    async fn connect(
        &mut self,
    ) -> io::Result<(
        ReadHalf<TlsStream<TcpStream>>,
        WriteHalf<TlsStream<TcpStream>>,
    )> {
        let socket = TcpStream::connect(self.addr).await?;
        let cx = tokio_rustls::TlsConnector::from(self.config.clone());
        let stream = cx.connect("dummy".try_into().unwrap(), socket).await?;
        Ok(split(stream))
    }

    pub async fn forwarding<R, W>(&mut self, incoming: Incoming<R, W>)
    where
        W: AsyncWrite + Unpin,
        R: AsyncRead + Unpin,
    {
        let (source, ir, iw) = match incoming {
            Incoming::Stream {
                source,
                reader,
                writer,
            } => (source, reader, writer),
        };

        match self.connect().await {
            Ok((or, ow)) => {
                debug!("forwarding {} --> TCP({})", source, self.addr);
                match io_process(ir, iw, or, ow).await {
                    Ok(_) => {}
                    Err(e) => error!(
                        "error at process {} --> TCP({}). detail: {}",
                        source, self.addr, e
                    ),
                }
            }
            Err(_) => todo!(),
        }
    }
}

pub async fn inbound_tcp(addr: SocketAddr, mut outbounds: Vec<Outbound>) -> io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("TCP listen at {}", addr);

    let mut index = 0;
    while let Ok((stream, rem_addr)) = listener.accept().await {
        debug!("TCP({}) incoming from {}", addr, rem_addr);

        if index >= outbounds.len() {
            index = 0;
        }
        let mut outbound = outbounds.get_mut(index).unwrap().clone();
        index += 1;

        tokio::spawn(async move {
            let (reader, writer) = split(stream);
            outbound
                .forwarding(Incoming::Stream {
                    source: format!("TCP({}/{})", addr, rem_addr),
                    reader,
                    writer,
                })
                .await
        });
    }

    Ok(())
}

pub async fn inbound_tls(
    addr: SocketAddr,
    certs: &Path,
    keys: &Path,
    mut outbounds: Vec<Outbound>,
) -> io::Result<()> {
    let certs = load_certs(certs)?;
    let mut keys = load_keys(keys)?;

    let config = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(certs, keys.remove(0))
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    let tls_acceptor = TlsAcceptor::from(Arc::new(config));

    let listener = TcpListener::bind(addr).await?;
    info!("TLS listen at {}", addr);

    let mut index = 0;
    while let Ok((stream, rem_addr)) = listener.accept().await {
        debug!("TLS({}) incoming from {}", addr, rem_addr);

        if index >= outbounds.len() {
            index = 0;
        }
        let mut outbound = outbounds.get_mut(index).unwrap().clone();
        index += 1;

        let tls_acceptor = tls_acceptor.clone();

        tokio::spawn(async move {
            match tls_acceptor.accept(stream).await {
                Ok(stream) => {
                    let (reader, writer) = split(stream);
                    outbound
                        .forwarding(Incoming::Stream {
                            source: format!("TCP({}/{})", addr, rem_addr),
                            reader,
                            writer,
                        })
                        .await;
                }
                Err(e) => {
                    error!(
                        "error at accept TLS({}) from {}. detail: {}",
                        addr, rem_addr, e
                    );
                }
            }
        });
    }

    Ok(())
}

fn load_certs(path: &Path) -> io::Result<Vec<Certificate>> {
    certs(&mut BufReader::new(std::fs::File::open(path)?))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid cert"))
        .map(|mut certs| certs.drain(..).map(Certificate).collect())
}

fn load_keys(path: &Path) -> io::Result<Vec<PrivateKey>> {
    rsa_private_keys(&mut BufReader::new(std::fs::File::open(path)?))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid key"))
        .map(|mut keys| keys.drain(..).map(PrivateKey).collect())
}
