use std::{
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use std::ops::DerefMut;

use log::{debug, error, info};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{channel, Sender},
        oneshot,
    },
};

use crate::{
    mux::{ChannelId, MuxConnection, MuxConnector},
    outbound::{io_process, Incoming, Outbound},
    pipe::{PipeReader, PipeWriter},
};

struct RevTcpListener {
    addr: SocketAddr,
    mux: Option<MuxConnection>,
}

impl RevTcpListener {
    pub fn bind(addr: SocketAddr) -> Self {
        Self { addr, mux: None }
    }

    pub async fn accept(&mut self) -> io::Result<(ChannelId, PipeReader, PipeWriter)> {
        loop {
            if self.mux.is_none() {
                let stream = match TcpStream::connect(self.addr).await {
                    Ok(o) => {
                        info!("connected to {}", self.addr);
                        o
                    }
                    Err(e) => {
                        error!("error at connect to {}. detail: {}", self.addr, e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                self.mux = Some(MuxConnection::new(stream));
            }

            let mux = self.mux.as_mut().unwrap();

            let res = match mux.accept().await {
                Ok(o) => {
                    debug!("incoming mux channel from {}", self.addr);
                    o
                }
                Err(e) => {
                    error!(
                        "error at listen mux channel from {}. detail: {}",
                        self.addr, e
                    );
                    self.mux = None;
                    continue;
                }
            };

            return Ok(res);
        }
    }
}

#[derive(Clone)]
pub struct RevTcpOutbound {
    addr: SocketAddr,
    conn_tx: Sender<oneshot::Sender<io::Result<(SocketAddr, MuxConnector)>>>,
}

impl RevTcpOutbound {
    pub fn bind(addr: SocketAddr) -> Self {
        let pool: Arc<Mutex<Vec<(SocketAddr, MuxConnection)>>> = Arc::new(Mutex::new(Vec::new()));
        let pool_ref = pool.clone();
        let (conn_tx, mut conn_rx) = channel(1024);

        tokio::spawn(async move {
            let mut index = 0;
            loop {
                let rq: oneshot::Sender<io::Result<(SocketAddr, MuxConnector)>> =
                    conn_rx.recv().await.unwrap();
                let mut pool = pool.lock().unwrap();

                pool.retain(|x| !x.1.is_closed());

                if pool.is_empty() {
                    rq.send(Err(io::Error::new(
                        io::ErrorKind::Other,
                        "no incoming connection",
                    )));
                    continue;
                }

                if pool.len() - 1 < index {
                    index = 0;
                }

                let item = pool.get_mut(index).unwrap();
                rq.send(Ok((item.0, item.1.create_connector())));

                index += 1;
            }
        });

        tokio::spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            while let Ok((stream, rem_addr)) = listener.accept().await {
                info!("incoming connection for {} from {}", addr, rem_addr);
                let mux = MuxConnection::new(stream);
                pool_ref.lock().unwrap().push((rem_addr, mux));
            }
        });

        Self { addr, conn_tx }
    }

    async fn connect(&mut self) -> io::Result<(SocketAddr, (ChannelId, PipeReader, PipeWriter))> {
        let (tx, rx) = oneshot::channel();
        self.conn_tx
            .send(tx)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "connection channel is die"))?;
        let (rem_addr, mut mux) = rx.await.unwrap()?;
        let channel = mux.connect().await?;
        Ok((rem_addr, channel))
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
            Ok((addr, (id, or, ow))) => {
                debug!("forwarding {} --> REVTCP({}/{})", source, addr, id);
                match io_process(ir, iw, or, ow).await {
                    Ok(_) => {}
                    Err(e) => error!(
                        "error at process {} --> REVTCP({}/{}). detail: {}",
                        source, addr, id, e
                    ),
                };
            }
            Err(e) => error!(
                "error at connect over REVTCP(listen: {}) for {}. detail: {}",
                self.addr, source, e
            ),
        }
    }
}

pub async fn inbound_revtcp(addr: SocketAddr, mut outbounds: Vec<Outbound>) -> io::Result<()> {
    let mut listener = RevTcpListener::bind(addr);
    info!("REVTCP listen at {}", addr);

    let mut index = 0;
    while let Ok((id, reader, writer)) = listener.accept().await {
        debug!("REVTCP({}) incoming from {}", addr, id);

        if index >= outbounds.len() {
            index = 0;
        }
        let mut outbound = outbounds.get_mut(index).unwrap().clone();
        index += 1;

        tokio::spawn(async move {
            outbound
                .forwarding(Incoming::Stream {
                    source: format!("REVTCP({}/{})", addr, id),
                    reader,
                    writer,
                })
                .await
        });
    }

    Ok(())
}
