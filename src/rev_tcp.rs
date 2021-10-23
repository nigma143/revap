use std::{borrow::Borrow, cmp, collections::HashMap, io, net::SocketAddr, sync::{
        atomic::{AtomicU32, AtomicUsize},
        Arc,
    }, time::Duration};

use log::{debug, error, info};
use tokio::{
    io::{split, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc::{self, error::TryRecvError, Receiver, Sender},
};

use crate::pipe::{self, PipeReader, PipeWriter};

enum Frame {
    New { id: u32, rem_w_size: u32 },
    Content { id: u32, payload: Vec<u8> },
    ContentProcessed { id: u32, remaining: u32 },
    Terminated { id: u32 },
}

async fn read_frame<R: AsyncRead + Unpin>(mut read: R) -> io::Result<Frame> {
    let opcode = read.read_u8().await?;
    let frame = match opcode {
        0x00 => {
            let id = read.read_u32().await?;
            let rem_w_size = read.read_u32().await?;

            Frame::New { id, rem_w_size }
        }
        0x01 => {
            let id = read.read_u32().await?;
            let len = read.read_u32().await?;
            let mut payload = Vec::new();
            unsafe {
                payload.set_len(len as usize);
            }

            read.read_exact(&mut payload).await?;

            Frame::Content { id, payload }
        }
        0x02 => {
            let id = read.read_u32().await?;
            let remaining = read.read_u32().await?;

            Frame::ContentProcessed { id, remaining }
        }
        0x03 => {
            let id = read.read_u32().await?;

            Frame::Terminated { id }
        }
        _ => Err(io::Error::new(
            io::ErrorKind::Other,
            format!("opcode `{}` not expected", opcode),
        ))?,
    };

    Ok(frame)
}

async fn write_frame<W: AsyncWrite + Unpin>(mut write: W, value: Frame) -> io::Result<()> {
    match value {
        Frame::New { id, rem_w_size } => {
            write.write_u8(0x01).await?;
            write.write_u32(id).await?;
            write.write_u32(rem_w_size).await?;
        }
        Frame::Content { id, payload } => {
            write.write_u8(0x02).await?;
            write.write_u32(id).await?;
            write.write_u32(payload.len() as u32).await?;
            write.write_all(&payload).await?;
        }
        Frame::ContentProcessed { id, remaining } => {
            write.write_u8(0x03).await?;
            write.write_u32(id).await?;
            write.write_u32(remaining).await?;
        }
        Frame::Terminated { id } => {
            write.write_u8(0x04).await?;
            write.write_u32(id).await?;
        }
    }

    Ok(())
}

const DEFAULT_REM_WIN_SIZE: u32 = 5 * 20 * 1024;

pub struct Multiplexor {
    message_tx: Sender<Frame>,
    connect_tx: Sender<Incoming>,
    listen_rx: Receiver<Outcoming>,
    last_id: Arc<AtomicU32>,
}

impl Multiplexor {
    pub fn new<S>(stream: S) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (read, write) = split(stream);

        let (message_tx, message_rx) = mpsc::channel(1024);
        let message_tx_ref = message_tx.clone();

        let (connect_tx, connect_rx) = mpsc::channel(1024);
        let (listen_tx, listen_rx) = mpsc::channel(1024);

        tokio::spawn(async move {
            write_loop(write, message_rx).await.unwrap();
        });
        tokio::spawn(async move { read_loop(read, message_tx_ref, connect_rx, listen_tx).await });

        Self {
            message_tx,
            connect_tx,
            listen_rx,
            last_id: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn is_closed(&self) -> bool {
        self.message_tx.is_closed() || self.connect_tx.is_closed()
    }

    pub async fn listen(&mut self) -> io::Result<(PipeReader, PipeWriter)> {
        let outcoming = self.listen_rx.recv().await.ok_or(io::Error::new(
            io::ErrorKind::Other,
            "listen channel is die",
        ))?;

        let w_size = outcoming
            .rem_w_size
            .load(std::sync::atomic::Ordering::Relaxed);
        let (write_out, read_out) = pipe::new_pipe(w_size);

        let message_tx = self.message_tx.clone();

        tokio::spawn(async move {
            output_conn_loop(message_tx, outcoming.id, read_out, outcoming.rem_w_size)
                .await
                .unwrap();
        });

        Ok((outcoming.rx, write_out))
    }

    pub async fn connect(&mut self) -> io::Result<(PipeReader, PipeWriter)> {
        let id = self
            .last_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let w_size = DEFAULT_REM_WIN_SIZE;

        let (write_in, read_in) = pipe::new_pipe(w_size as usize);
        let (write_out, read_out) = pipe::new_pipe(w_size as usize);

        self.message_tx
            .send(Frame::New {
                id,
                rem_w_size: w_size.clone(),
            })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "message channel is die"))?;

        let w_size = Arc::new(AtomicUsize::new(w_size as usize));

        let incoming = Incoming {
            id,
            tx: write_in,
            rem_w_size: w_size.clone(),
        };

        self.connect_tx
            .send(incoming)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "listen channel is die"))?;

        let message_tx = self.message_tx.clone();

        tokio::spawn(async move {
            output_conn_loop(message_tx, id, read_out, w_size)
                .await
                .unwrap();
        });

        Ok((read_in, write_out))
    }
}

async fn output_conn_loop(
    message_tx: Sender<Frame>,
    id: u32,
    mut reader: PipeReader,
    rem_w_size: Arc<AtomicUsize>,
) -> io::Result<()> {
    let mut buf = [0_u8; 2 * 1024];
    loop {
        let size = rem_w_size.load(std::sync::atomic::Ordering::Relaxed);
        let size = cmp::min(size as usize, buf.len());
        if size <= 0 {
            tokio::task::yield_now().await;
            continue;
        }
        match reader.read(&mut buf[0..size]).await {
            Ok(n) => {
                if n == 0 {
                    message_tx
                        .send(Frame::Terminated { id })
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "message channel is die")
                        })?;
                    return Ok(());
                } else {
                    rem_w_size.fetch_sub(n, std::sync::atomic::Ordering::Relaxed);
                    message_tx
                        .send(Frame::Content {
                            id,
                            payload: buf[0..n].to_vec(),
                        })
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "message channel is die")
                        })?;
                }
            }
            Err(_) => {
                message_tx
                    .send(Frame::Terminated { id })
                    .await
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "message channel is die"))?;
                return Ok(());
            }
        };
    }

    unreachable!();
}

struct Incoming {
    id: u32,
    tx: PipeWriter,
    rem_w_size: Arc<AtomicUsize>,
}

struct Outcoming {
    id: u32,
    rx: PipeReader,
    rem_w_size: Arc<AtomicUsize>,
}

async fn write_loop<W>(mut write: W, mut message_rx: Receiver<Frame>) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    loop {
        match message_rx.recv().await {
            Some(msg) => {
                write_frame(&mut write, msg.into()).await?;
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "message channel is die",
                ))
            }
        }
    }

    unreachable!();
}

async fn read_loop<R>(
    mut read: R,
    message_tx: Sender<Frame>,
    mut connect_rx: Receiver<Incoming>,
    listen_tx: Sender<Outcoming>,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut map = HashMap::new();
    loop {
        match read_frame(&mut read).await? {
            Frame::New { id, rem_w_size } => {
                let (tx, rx) = pipe::new_pipe(rem_w_size as usize);
                let rem_w_size = Arc::new(AtomicUsize::new(rem_w_size as usize));

                listen_tx
                    .send(Outcoming {
                        id,
                        rx,
                        rem_w_size: rem_w_size.clone(),
                    })
                    .await
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "listen channel is die"))?;

                map.insert(
                    id,
                    Incoming {
                        id,
                        tx,
                        rem_w_size: rem_w_size.clone(),
                    },
                );
            }
            Frame::Content { id, payload } => loop {
                match connect_rx.try_recv() {
                    Ok(c) => map.insert(c.id, c),
                    Err(e) => match e {
                        TryRecvError::Empty => break,
                        TryRecvError::Disconnected => Err(io::Error::new(
                            io::ErrorKind::Other,
                            "listen channel is closed",
                        ))?,
                    },
                };
                match map.get_mut(&id) {
                    Some(c) => match c.tx.write_all(&payload).await {
                        Ok(_) => {
                            message_tx
                                .send(Frame::ContentProcessed {
                                    id,
                                    remaining: c.tx.remaining() as u32,
                                })
                                .await
                                .map_err(|_| {
                                    io::Error::new(io::ErrorKind::Other, "message channel is die")
                                })?;
                        }
                        Err(_) => {
                            map.remove(&id).unwrap();
                            message_tx
                                .send(Frame::Terminated { id })
                                .await
                                .map_err(|_| {
                                    io::Error::new(io::ErrorKind::Other, "message channel is die")
                                })?;
                        }
                    },
                    None => error!("unknown connect `{}`", id),
                }
            },
            Frame::ContentProcessed { id, remaining } => match map.get_mut(&id) {
                Some(c) => {
                    c.rem_w_size
                        .store(remaining as usize, std::sync::atomic::Ordering::Relaxed);
                }
                None => error!("unknown connect `{}`", id),
            },
            Frame::Terminated { id } => match map.remove(&id) {
                Some(c) => drop(c),
                None => error!("unknown connect `{}`", id),
            },
        }
    }

    unreachable!();
}

pub struct RevTcpConnector {

}

impl RevTcpConnector {
    pub fn bind(addr: SocketAddr) -> Self {
        
    }
}

pub struct RevTcpListener {
    addr: SocketAddr,
    mux: Option<Multiplexor>,
}

impl RevTcpListener {
    pub fn bind(addr: SocketAddr) -> Self {
        Self { addr, mux: None }
    }

    pub async fn accept(&mut self) -> io::Result<(PipeReader, PipeWriter)> {
        loop {
            if self.mux.is_none() {
                let stream = match TcpStream::connect(self.addr).await {
                    Ok(o) => {
                        info!("connected to `{}`", self.addr);
                        o
                    },
                    Err(e) => {
                        error!("error on connect to `{}`. detail: {}", self.addr, e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                self.mux = Some(Multiplexor::new(stream));
            }

            let mux = self.mux.as_mut().unwrap();

            let res = match mux.listen().await {
                Ok(o) => {
                    debug!("incoming mux channel from `{}`", self.addr);
                    o
                },
                Err(e) => {
                    error!("error on listen mux channel from `{}`. detail: {}", self.addr, e);
                    continue;
                },
            };

            return Ok(res);
        }
    }
}
