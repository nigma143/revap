use std::{
    cmp,
    collections::HashMap,
    io,
    sync::{atomic::AtomicUsize, Arc},
};

use id_pool::IdPool;
use log::debug;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    select,
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
};

use crate::pipe::{self, PipeReader, PipeWriter};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum ChannelId {
    Local(u32),
    Remote(u32),
}

enum Frame {
    New { id: ChannelId, rem_w_size: u32 },
    Content { id: ChannelId, payload: Vec<u8> },
    ContentProcessed { id: ChannelId, remaining: u32 },
    Terminated { id: ChannelId },
}

impl std::fmt::Display for ChannelId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelId::Local(id) => write!(f, "Local({})", id),
            ChannelId::Remote(id) => write!(f, "Remote({})", id),
        }
    }
}

impl From<i32> for ChannelId {
    fn from(value: i32) -> Self {
        if value >= 0 {
            ChannelId::Local(value as u32)
        } else {
            ChannelId::Remote(value.abs() as u32)
        }
    }
}

impl Into<i32> for ChannelId {
    fn into(self) -> i32 {
        match self {
            ChannelId::Local(id) => -1 * (id as i32),
            ChannelId::Remote(id) => id as i32,
        }
    }
}

impl std::fmt::Display for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Frame::New { id, rem_w_size } => {
                write!(f, "New(id: {}, rem_w_size: {})", id, rem_w_size)
            }
            Frame::Content { id, payload } => {
                write!(f, "Content(id: {}, payload_len: {})", id, payload.len())
            }
            Frame::ContentProcessed { id, remaining } => {
                write!(f, "ContentProcessed(id: {}, remaining: {})", id, remaining)
            }
            Frame::Terminated { id } => write!(f, "Terminated(id: {})", id),
        }
    }
}

async fn read_frame<R: AsyncRead + Unpin>(mut read: R) -> io::Result<Frame> {
    let opcode = read.read_u8().await?;
    let frame = match opcode {
        0x01 => {
            let id = read.read_i32().await?.into();
            let rem_w_size = read.read_u32().await?;
            Frame::New { id, rem_w_size }
        }
        0x02 => {
            let id = read.read_i32().await?.into();
            let len = read.read_u32().await? as usize;
            let mut payload = Vec::with_capacity(len);
            unsafe {
                payload.set_len(len);
            }
            read.read_exact(&mut payload).await?;
            Frame::Content { id, payload }
        }
        0x03 => {
            let id = read.read_i32().await?.into();
            let remaining = read.read_u32().await?;
            Frame::ContentProcessed { id, remaining }
        }
        0x04 => {
            let id = read.read_i32().await?.into();
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
            write.write_i32(id.into()).await?;
            write.write_u32(rem_w_size).await?;
        }
        Frame::Content { id, payload } => {
            write.write_u8(0x02).await?;
            write.write_i32(id.into()).await?;
            write.write_u32(payload.len() as u32).await?;
            write.write_all(&payload).await?;
        }
        Frame::ContentProcessed { id, remaining } => {
            write.write_u8(0x03).await?;
            write.write_i32(id.into()).await?;
            write.write_u32(remaining).await?;
        }
        Frame::Terminated { id } => {
            write.write_u8(0x04).await?;
            write.write_i32(id.into()).await?;
        }
    }

    Ok(())
}

pub struct MuxConnection {
    w_frame_tx: Sender<Frame>,
    listen_rx: Receiver<ChannelRx>,
    new_tx: Sender<oneshot::Sender<ChannelRx>>,
    term_tx: Sender<ChannelId>,
}

impl MuxConnection {
    pub fn new<S>(stream: S) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (read, write) = tokio::io::split(stream);

        let (w_frame_tx, w_frame_rx) = mpsc::channel(1024);
        let w_frame_tx_ref = w_frame_tx.clone();

        let (listen_tx, listen_rx) = mpsc::channel(1024);
        let (new_tx, new_rx) = mpsc::channel(1024);
        let (term_tx, term_rx) = mpsc::channel(1024);

        tokio::spawn(async move { write_loop(write, w_frame_rx).await });
        tokio::spawn(
            async move { read_loop(read, w_frame_tx_ref, listen_tx, new_rx, term_rx).await },
        );

        Self {
            w_frame_tx,
            listen_rx,
            new_tx,
            term_tx,
        }
    }

    pub async fn accept(&mut self) -> io::Result<(ChannelId, PipeReader, PipeWriter)> {
        let channel = self.listen_rx.recv().await.ok_or(io::Error::new(
            io::ErrorKind::Other,
            "tx listen channel is die",
        ))?;

        let w_size = channel
            .rem_w_size
            .load(std::sync::atomic::Ordering::Relaxed);
        let (wo, ro) = pipe::new_pipe(w_size);

        let w_frame_tx = self.w_frame_tx.clone();
        let outcoming = Outcoming {
            id: channel.id,
            rem_w_size: channel.rem_w_size.clone(),
            rx: ro,
        };
        let term_tx = self.term_tx.clone();

        tokio::spawn(async move { conn_loop(w_frame_tx, term_tx, outcoming).await });

        Ok((channel.id, channel.reader, wo))
    }

    pub fn create_connector(&mut self) -> MuxConnector {
        MuxConnector {
            w_frame_tx: self.w_frame_tx.clone(),
            new_tx: self.new_tx.clone(),
            term_tx: self.term_tx.clone(),
        }
    }

    pub fn is_closed(&self) -> bool {
        self.w_frame_tx.is_closed() || self.new_tx.is_closed()
    }
}

pub struct MuxConnector {
    w_frame_tx: Sender<Frame>,
    new_tx: Sender<oneshot::Sender<ChannelRx>>,
    term_tx: Sender<ChannelId>,
}

impl MuxConnector {
    pub async fn connect(&mut self) -> io::Result<(ChannelId, PipeReader, PipeWriter)> {
        let (in_tx, in_rx) = oneshot::channel();

        self.new_tx
            .send(in_tx)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "rx new channel is die"))?;

        let channel = in_rx.await.unwrap();

        let w_size = channel
            .rem_w_size
            .load(std::sync::atomic::Ordering::Relaxed);

        let (wo, ro) = pipe::new_pipe(w_size as usize);

        let rem_w_size = Arc::new(AtomicUsize::new(w_size as usize));

        let w_frame_tx = self.w_frame_tx.clone();
        let outcoming = Outcoming {
            id: channel.id,
            rx: ro,
            rem_w_size: rem_w_size,
        };
        let term_tx = self.term_tx.clone();

        tokio::spawn(async move { conn_loop(w_frame_tx, term_tx, outcoming).await });

        Ok((channel.id, channel.reader, wo))
    }
}

async fn conn_loop(
    w_frame_tx: Sender<Frame>,
    term_tx: Sender<ChannelId>,
    mut outcoming: Outcoming,
) -> io::Result<()> {
    let mut buf = [0_u8; 2 * 1024];
    loop {
        let size = outcoming
            .rem_w_size
            .load(std::sync::atomic::Ordering::Relaxed);
        let size = cmp::min(size as usize, buf.len());
        if size <= 0 {
            tokio::task::yield_now().await;
            continue;
        }
        match outcoming.rx.read(&mut buf[0..size]).await {
            Ok(n) => {
                if n == 0 {
                    term_tx.send(outcoming.id).await.map_err(|_| {
                        io::Error::new(io::ErrorKind::Other, "rx term channel is die")
                    })?;
                    return Ok(());
                } else {
                    outcoming
                        .rem_w_size
                        .fetch_sub(n, std::sync::atomic::Ordering::Relaxed);
                    w_frame_tx
                        .send(Frame::Content {
                            id: outcoming.id,
                            payload: buf[0..n].to_vec(),
                        })
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "rx frame channel is die")
                        })?;
                }
            }
            Err(_) => {
                term_tx
                    .send(outcoming.id)
                    .await
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "rx term channel is die"))?;
                return Ok(());
            }
        };
    }
}

async fn write_loop<W>(mut write: W, mut message_rx: Receiver<Frame>) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    loop {
        let frame = message_rx.recv().await.ok_or(io::Error::new(
            io::ErrorKind::Other,
            "tx frame channel is die",
        ))?;
        debug!("<- {}", frame);
        write_frame(&mut write, frame).await?;
    }
}

async fn read_loop<R>(
    mut read: R,
    w_frame_tx: Sender<Frame>,
    listen_tx: Sender<ChannelRx>,
    mut new_rx: Receiver<oneshot::Sender<ChannelRx>>,
    mut term_rx: Receiver<ChannelId>,
) -> io::Result<()>
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let mut map = HashMap::new();
    let mut ids_pool = IdPool::new();
    let (r_frame_tx, mut r_frame_rx) = mpsc::channel(1024);

    tokio::spawn(async move {
        loop {
            let frame = read_frame(&mut read).await?;
            debug!("-> {}", frame);
            r_frame_tx.send(frame).await.map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "rx read frame channel is die")
            })?;
        }
        io::Result::Ok(())
    });

    loop {
        select! {
            res = new_rx.recv() => {
                match res {
                    Some(shot) => {
                        let id = ChannelId::Local(ids_pool.request_id().unwrap());
                        let w_size = DEFAULT_REM_WIN_SIZE;
                        let (wi, ri) = pipe::new_pipe(w_size as usize);
                        let rem_w_size = Arc::new(AtomicUsize::new(w_size as usize));
                        map.insert(
                            id,
                            ChannelTx {
                                id,
                                writer: wi,
                                rem_w_size: rem_w_size.clone(),
                                state: ChannelState::Established,
                            },
                        );
                        w_frame_tx
                            .send(Frame::New {
                                id,
                                rem_w_size: w_size,
                            })
                            .await
                            .map_err(|_| {
                                io::Error::new(io::ErrorKind::Other, "rx write frame channel is die")
                            })?;
                        shot.send(ChannelRx {
                            id,
                            reader: ri,
                            rem_w_size: rem_w_size.clone(),
                        })
                        .map_err(|_| io::Error::new(io::ErrorKind::Other, "rx new channel is die"))?;
                    }
                    None => break,
                }
            },
            res = term_rx.recv() => {
                match res {
                    Some(id) => {
                        let channel = map.get_mut(&id).unwrap();
                        match channel.state {
                            ChannelState::Established => {
                                channel.state = ChannelState::FinWait;
                            },
                            ChannelState::FinWait => {
                                map.remove(&id).unwrap();
                                if let ChannelId::Local(id) = id {
                                    ids_pool.return_id(id).unwrap();
                                }
                            }
                        };
                        w_frame_tx.send(Frame::Terminated { id }).await
                            .map_err(|_| { io::Error::new(io::ErrorKind::Other,"rx write frame channel is die",)})?;
                    },
                    None => break,
                }
            },
            res = r_frame_rx.recv() => {
                match res {
                    Some(frame) => match frame {
                        Frame::New { id, rem_w_size } => {
                            let (wi, ri) = pipe::new_pipe(rem_w_size as usize);
                            let rem_w_size = Arc::new(AtomicUsize::new(rem_w_size as usize));
                            listen_tx
                                .send(ChannelRx {
                                    id,
                                    reader: ri,
                                    rem_w_size: rem_w_size.clone(),
                                })
                                .await
                                .map_err(|_| {
                                    io::Error::new(io::ErrorKind::Other, "rx listen channel is die")
                                })?;
                            map.insert(
                                id,
                                ChannelTx {
                                    id,
                                    writer: wi,
                                    rem_w_size: rem_w_size.clone(),
                                    state: ChannelState::Established,
                                },
                            );
                        }
                        Frame::Content { id, payload } => match map.get_mut(&id) {
                            Some(ChannelTx { id, writer, .. }) => match writer.write_all(&payload).await {
                                Ok(_) => {
                                    w_frame_tx
                                        .send(Frame::ContentProcessed {
                                            id: *id,
                                            remaining: writer.remaining() as u32,
                                        })
                                        .await
                                        .map_err(|_| {
                                            io::Error::new(io::ErrorKind::Other, "rx frame channel is die")
                                        })?;
                                }
                                Err(e) => {
                                    debug!("id: {} pipe reader is die. {}", id, e);
                                }
                            },
                            None => debug!("id: {} not found", id),
                        },
                        Frame::ContentProcessed { id, remaining } => match map.get_mut(&id) {
                            Some(c) => {
                                c.rem_w_size
                                    .store(remaining as usize, std::sync::atomic::Ordering::Relaxed);
                            }
                            None => debug!("id: {} not found", id),
                        },
                        Frame::Terminated { id } => {
                            let channel = map.get_mut(&id).unwrap();
                            match channel.state {
                                ChannelState::Established => {
                                    channel.state = ChannelState::FinWait;
                                    channel.writer.shutdown().await.unwrap();
                                }
                                ChannelState::FinWait => {
                                    map.remove(&id).unwrap();
                                    if let ChannelId::Local(id) = id {
                                        ids_pool.return_id(id).unwrap();
                                    }
                                }
                            }
                        }
                    },
                    None => break,
                }
            }
        };
    }

    Ok(())
}

const DEFAULT_REM_WIN_SIZE: u32 = 5 * 20 * 1024;

struct Incoming {
    id: ChannelId,
    tx: PipeWriter,
    rem_w_size: Arc<AtomicUsize>,
}

struct Outcoming {
    id: ChannelId,
    rx: PipeReader,
    rem_w_size: Arc<AtomicUsize>,
}

struct ChannelTx {
    id: ChannelId,
    writer: PipeWriter,
    rem_w_size: Arc<AtomicUsize>,
    state: ChannelState,
}

struct ChannelRx {
    id: ChannelId,
    reader: PipeReader,
    rem_w_size: Arc<AtomicUsize>,
}

enum ChannelState {
    Established,
    FinWait,
}
