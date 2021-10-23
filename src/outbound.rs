use std::{fmt, io, net::SocketAddr, sync::Arc};

use rustls::ClientConfig;
use tokio::{
    io::{split, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf},
    net::TcpStream,
};
use tokio_rustls::client::TlsStream;

pub enum Incoming<R, W> {
    Tcp {
        remoute_addr: SocketAddr,
        reader: R,
        writer: W,
    },
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
    pub async fn forwarding<R, W>(&mut self, incoming: Incoming<R, W>) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
        R: AsyncRead + Unpin,
    {
        let (in_reader, in_writer) = match incoming {
            Incoming::Tcp {
                remoute_addr,
                reader,
                writer,
            } => (reader, writer),
        };

        match self {
            Outbound::Tcp(tcp) => {
                let (out_reader, out_writer) = tcp.connect().await?;
                self.process(in_reader, in_writer, out_reader, out_writer)
                    .await?;
            }
            Outbound::Tls(tls) => {
                let (out_reader, out_writer) = tls.connect().await?;
                self.process(in_reader, in_writer, out_reader, out_writer)
                    .await?;
            }
        };

        Ok(())
    }

    async fn process<R, W, OR, OW>(
        &mut self,
        mut in_reader: R,
        mut in_writer: W,
        mut out_reader: OR,
        mut out_writer: OW,
    ) -> io::Result<()>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
        OR: AsyncRead + Unpin,
        OW: AsyncWrite + Unpin,
    {
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

    async fn connect(&mut self) -> io::Result<(ReadHalf<TcpStream>, WriteHalf<TcpStream>)> {
        let stream = TcpStream::connect(self.addr).await?;
        Ok(split(stream))
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
}
