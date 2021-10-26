use std::{
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use log::{debug, error, info};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{channel, Sender},
        oneshot,
    },
};

use crate::{
    multiplexor::{ChannelId, Multiplexor, MultiplexorConnector},
    outbound::{Incoming, Outbound},
    pipe::{PipeReader, PipeWriter},
};

#[derive(Clone)]
pub struct RevTcpOutbound {
    conn_tx: Sender<oneshot::Sender<io::Result<MultiplexorConnector>>>,
}

impl RevTcpOutbound {
    pub fn bind(addr: SocketAddr) -> Self {
        let pool: Arc<Mutex<Vec<Multiplexor>>> = Arc::new(Mutex::new(Vec::new()));
        let pool_ref = pool.clone();
        let (conn_tx, mut conn_rx) = channel(1024);

        tokio::spawn(async move {
            let mut index = 0;
            loop {
                let rq: oneshot::Sender<io::Result<MultiplexorConnector>> =
                    conn_rx.recv().await.unwrap();
                let mut pool = pool.lock().unwrap();

                if pool.is_empty() {
                    rq.send(Err(io::Error::new(
                        io::ErrorKind::Other,
                        "no incoming connection",
                    )));
                    continue;
                }

                pool.retain(|x| !x.is_closed());

                if pool.len() - 1 < index {
                    index = 0;
                }

                let mux = pool.get(index).unwrap();
                rq.send(Ok(mux.get_connector()));

                index += 1;
            }
        });

        tokio::spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            while let Ok((stream, rem_addr)) = listener.accept().await {
                info!("incoming connection {}", rem_addr);
                let mux = Multiplexor::new(stream);
                pool_ref.lock().unwrap().push(mux);
            }
        });

        Self { conn_tx }
    }

    pub async fn connect(&mut self) -> io::Result<(ChannelId, PipeReader, PipeWriter)> {
        let (tx, rx) = oneshot::channel();
        self.conn_tx
            .send(tx)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "connection channel is die"))?;
        let channel = rx.await.unwrap()?.connect().await?;
        Ok(channel)
    }
}

struct RevTcpListener {
    addr: SocketAddr,
    mux: Option<Multiplexor>,
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
                        error!("error on connect to {}. detail: {}", self.addr, e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                self.mux = Some(Multiplexor::new(stream));
            }

            let mux = self.mux.as_mut().unwrap();

            let res = match mux.listen().await {
                Ok(o) => {
                    debug!("incoming mux channel from {}", self.addr);
                    o
                }
                Err(e) => {
                    error!(
                        "error on listen mux channel from {}. detail: {}",
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

pub async fn inbound_revtcp(addr: SocketAddr, mut outbounds: Vec<Outbound>) -> io::Result<()> {
    let mut listener = RevTcpListener::bind(addr);
    info!("revtcp listen at {}", addr);

    let mut index = 0;
    while let Ok((id, reader, writer)) = listener.accept().await {
        debug!("revtcp channe {} from {}", id, addr);

        if index >= outbounds.len() {
            index = 0;
        }
        let mut outbound = outbounds.get_mut(index).unwrap().clone();
        index += 1;

        tokio::spawn(async move {
            debug!("forwarding revtcp({}/{}) --> {}", addr, id, outbound);
            let r = outbound
                .forwarding(Incoming::Tcp {
                    remoute_addr: addr,
                    reader,
                    writer,
                })
                .await;
            if let Err(e) = r {
                error!(
                    "error on process revtcp({}/{}) --> {}. detail: {}",
                    addr, id, outbound, e
                );
            }
        });
    }

    Ok(())
}
