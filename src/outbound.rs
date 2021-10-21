use std::{fmt, io, net::SocketAddr, sync::Arc};

use rustls::ClientConfig;
use tokio::{
    io::{split, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::client::TlsStream;

pub enum Incoming<S> {
    Tcp { remoute_addr: SocketAddr, stream: S },
}

#[derive(Clone)]
pub enum Outbound {
    Tcp(TcpOutbound),
    Tls(TlsOutbound),
}

impl fmt::Display for Outbound {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Outbound::Tcp(t) => write!(f, "tcp({})", t.addr),
            Outbound::Tls(t) => write!(f, "tls({})", t.addr),
        }
    }
}

impl Outbound {
    pub async fn forwarding<S>(&mut self, incoming: Incoming<S>) -> io::Result<()>
    where
        S: AsyncRead + AsyncWrite,
    {
        let incoming = match incoming {
            Incoming::Tcp {
                remoute_addr,
                stream,
            } => stream,
        };

        match self {
            Outbound::Tcp(tcp) => {
                let outcoming = tcp.connect().await?;
                self.process(incoming, outcoming).await?;
            }
            Outbound::Tls(tls) => {
                let outcoming = tls.connect().await?;
                self.process(incoming, outcoming).await?;
            }
        };

        Ok(())
    }

    async fn process<I, O>(&mut self, incoming: I, outcoming: O) -> io::Result<()>
    where
        I: AsyncRead + AsyncWrite,
        O: AsyncRead + AsyncWrite,
    {
        let (mut in_reader, mut in_writer) = split(incoming);
        let (mut out_reader, mut out_writer) = split(outcoming);

        let client_to_server = async {
            let mut buf = vec![0; 2 * 1024].into_boxed_slice();
            loop {
                let n = in_reader.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                out_writer.write_all(&buf[0..n]).await?;
            }
            out_writer.shutdown().await
        };

        let server_to_client = async {
            let mut buf = vec![0; 2 * 1024].into_boxed_slice();
            loop {
                let n = out_reader.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                in_writer.write_all(&buf[0..n]).await?;
            }
            in_writer.shutdown().await
        };

        tokio::try_join!(client_to_server, server_to_client)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct TcpOutbound {
    addr: SocketAddr,
}

impl TcpOutbound {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    async fn connect(&mut self) -> io::Result<TcpStream> {
        TcpStream::connect(self.addr).await
    }
}

#[derive(Clone)]
pub struct TlsOutbound {
    addr: SocketAddr,
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

    async fn connect(&mut self) -> io::Result<TlsStream<TcpStream>> {
        let socket = TcpStream::connect(self.addr).await?;
        let cx = tokio_rustls::TlsConnector::from(self.config.clone());
        cx.connect("dummy".try_into().unwrap(), socket).await
    }
}
