use crate::pipe::{self, PipeArbiter, PipeReader, PipeWriter};
use id_pool::IdPool;
use std::{
    cmp,
    collections::HashMap,
    error::Error,
    io,
    sync::{atomic::AtomicUsize, Arc},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    select,
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum ChannelId {
    Local(u32),
    Remote(u32),
}

#[derive(Debug)]
enum Frame {
    New { id: ChannelId, rem_w_size: u32 },
    Content { id: ChannelId, payload: Vec<u8> },
    ContentProcessed { id: ChannelId, processed: u32 },
    WriteComplited { id: ChannelId },
    ReadComplited { id: ChannelId },
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
            Frame::ContentProcessed { id, processed } => {
                write!(f, "ContentProcessed(id: {}, processed: {})", id, processed)
            }
            Frame::WriteComplited { id } => write!(f, "WriteComplited(id: {})", id),
            Frame::ReadComplited { id } => write!(f, "ReadComplited(id: {})", id),
        }
    }
}

const OP_NEW: u8 = 0x01;
const OP_CONTENT: u8 = 0x02;
const OP_PROCESSED: u8 = 0x03;
const OP_WRITE_COMPLITED: u8 = 0x04;
const OP_READ_COMPLITED: u8 = 0x05;

async fn write_frame<W: AsyncWrite + Unpin>(mut write: W, value: Frame) -> io::Result<()> {
    match value {
        Frame::New { id, rem_w_size } => {
            let mut buf = [0; 9];
            buf[0] = OP_NEW;
            buf[1..5].copy_from_slice(&Into::<i32>::into(id).to_be_bytes()[..]);
            buf[5..9].copy_from_slice(&rem_w_size.to_be_bytes()[..]);
            write.write_all(&buf).await?;
        }
        Frame::Content { id, payload } => {
            if payload.len() > MAX_PAYLOAD_SIZE {
                return Err(io::Error::new(io::ErrorKind::Other, "payload too large"));
            }
            let mut buf = [0; 9];
            buf[0] = OP_CONTENT;
            buf[1..5].copy_from_slice(&Into::<i32>::into(id).to_be_bytes()[..]);
            buf[5..9].copy_from_slice(&(payload.len() as u32).to_be_bytes()[..]);
            write.write_all(&buf).await?;
            write.write_all(&payload).await?;
        }
        Frame::ContentProcessed { id, processed } => {
            let mut buf = [0; 9];
            buf[0] = OP_PROCESSED;
            buf[1..5].copy_from_slice(&Into::<i32>::into(id).to_be_bytes()[..]);
            buf[5..9].copy_from_slice(&processed.to_be_bytes()[..]);
            write.write_all(&buf).await?;
        }
        Frame::WriteComplited { id } => {
            let mut buf = [0; 5];
            buf[0] = OP_WRITE_COMPLITED;
            buf[1..5].copy_from_slice(&Into::<i32>::into(id).to_be_bytes()[..]);
            write.write_all(&buf).await?;
        }
        Frame::ReadComplited { id } => {
            let mut buf = [0; 5];
            buf[0] = OP_READ_COMPLITED;
            buf[1..5].copy_from_slice(&Into::<i32>::into(id).to_be_bytes()[..]);
            write.write_all(&buf).await?;
        }
    }
    Ok(())
}

async fn read_frame<R: AsyncRead + Unpin>(mut read: R) -> io::Result<Frame> {
    let mut buf = [0; 5];
    read.read_exact(&mut buf).await?;
    let opcode = buf[0];
    let mut id_buf = [0; 4];
    id_buf.copy_from_slice(&mut buf[1..5]);
    let id = i32::from_be_bytes(id_buf).into();

    let frame = match opcode {
        OP_NEW => {
            let rem_w_size = read.read_u32().await?;
            Frame::New { id, rem_w_size }
        }
        OP_CONTENT => {
            let len = read.read_u32().await? as usize;
            if len > MAX_PAYLOAD_SIZE {
                return Err(io::Error::new(io::ErrorKind::Other, "payload too large"));
            }
            let mut payload = Vec::with_capacity(len);
            unsafe {
                payload.set_len(len);
            }
            read.read_exact(&mut payload).await?;
            Frame::Content { id, payload }
        }
        OP_PROCESSED => {
            let processed = read.read_u32().await?;
            Frame::ContentProcessed { id, processed }
        }
        OP_WRITE_COMPLITED => Frame::WriteComplited { id },
        OP_READ_COMPLITED => Frame::ReadComplited { id },
        _ => Err(io::Error::new(
            io::ErrorKind::Other,
            format!("opcode `{}` not expected", opcode),
        ))?,
    };
    Ok(frame)
}

pub struct MuxConnection {
    listen: Receiver<Channel>,
    new: Sender<oneshot::Sender<Channel>>,
}

impl MuxConnection {
    pub fn new<S>(stream: S) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (read, write) = tokio::io::split(stream);
        let (listen_tx, listen_rx) = mpsc::channel(1024);
        let (new_tx, new_rx) = mpsc::channel(1024);

        tokio::spawn(async move {
            if let Err(e) = connection_loop(write, read, listen_tx, new_rx).await {
                log::trace!("connection loop end with error: {}", e);
            }
        });

        Self {
            listen: listen_rx,
            new: new_tx,
        }
    }

    pub async fn accept(&mut self) -> io::Result<(ChannelId, PipeReader, PipeWriter)> {
        let channel = self.listen.recv().await.ok_or(io::Error::new(
            io::ErrorKind::Other,
            "can't accept channel because transport is die",
        ))?;

        Ok((channel.id, channel.ri, channel.wo))
    }

    pub fn create_connector(&mut self) -> MuxConnector {
        MuxConnector {
            new: self.new.clone(),
        }
    }

    pub fn is_closed(&self) -> bool {
        self.new.is_closed()
    }
}

pub struct MuxConnector {
    new: Sender<oneshot::Sender<Channel>>,
}

impl MuxConnector {
    pub async fn connect(&mut self) -> io::Result<(ChannelId, PipeReader, PipeWriter)> {
        let (in_tx, in_rx) = oneshot::channel();

        self.new.send(in_tx).await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "can't create channel because transport is die",
            )
        })?;

        let channel = in_rx.await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "error at connecting because transport is die",
            )
        })?;

        Ok((channel.id, channel.ri, channel.wo))
    }
}

async fn connection_loop<R, W>(
    mut write: W,
    mut read: R,
    listen: Sender<Channel>,
    mut new: Receiver<oneshot::Sender<Channel>>,
) -> Result<(), Box<dyn Error>>
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    let mut map = HashMap::new();
    let mut ids_pool = IdPool::new();

    let (output_tx, mut output_rx) = mpsc::channel(1024);
    tokio::spawn(async move {
        if let Err(e) = async move {
            loop {
                let frame = output_rx.recv().await.ok_or("recv channel closed")?;
                log::trace!("<- {}", frame);
                write_frame(&mut write, frame).await?;
            }
            #[allow(unreachable_code)]
            Result::<(), Box<dyn Error>>::Ok(())
        }
        .await
        {
            log::trace!("transport write loop end with error: {}", e);
        }
    });

    let (input_tx, mut input_rx) = mpsc::channel(1024);
    tokio::spawn(async move {
        if let Err(e) = async move {
            loop {
                let frame = read_frame(&mut read).await?;
                log::trace!("-> {}", frame);
                input_tx.send(frame).await?;
            }
            #[allow(unreachable_code)]
            Result::<(), Box<dyn Error>>::Ok(())
        }
        .await
        {
            log::trace!("transport read loop end with error: {}", e);
        }
    });

    fn crate_channel(
        id: ChannelId,
        w_size: usize,
        rem_w_size: Arc<AtomicUsize>,
    ) -> (ChannelRef, PipeReader, Channel) {
        let (ai, wi, ri) = pipe::new_pipe2(w_size);
        let (ao, wo, ro) = pipe::new_pipe2(w_size);
        (
            ChannelRef {
                in_arbiter: ai,
                out_arbiter: ao,
                input: wi,
                input_processed: 0,
                output_w_size: rem_w_size.clone(),
                read_flag: true,
                rem_write_flag: true,
                rem_read_flag: true,
            },
            ro,
            Channel { id, wo, ri },
        )
    }

    loop {
        select! {
            res = new.recv() => {
                match res {
                    Some(shot) => {
                        let id = ChannelId::Local(ids_pool.request_id().unwrap());
                        let w_size = WIN_SIZE;
                        let rem_w_size = Arc::new(AtomicUsize::new(w_size));
                        let (channel_tx, ro, channel_rx) = crate_channel(id, w_size, rem_w_size.clone());
                        map.insert(id, channel_tx);
                        output_tx
                            .send(Frame::New {
                                id,
                                rem_w_size: w_size as u32,
                            })
                            .await?;
                        spawn_channel_read(id, ro, output_tx.clone(), rem_w_size.clone()); //
                        let _ = shot.send(channel_rx);
                    }
                    None => break,
                }
            },
            res = input_rx.recv() => {
                match res {
                    Some(frame) => match frame {
                        Frame::New { id, rem_w_size } => {
                            let rem_w_size = Arc::new(AtomicUsize::new(rem_w_size as usize));
                            let (channel_tx, ro, channel_rx) = crate_channel(id, WIN_SIZE, rem_w_size.clone());
                            map.insert(id, channel_tx);
                            spawn_channel_read(id, ro, output_tx.clone(), rem_w_size.clone());
                            listen.send(channel_rx).await?;
                        }
                        Frame::Content { id, payload } => {
                            let channel = map.get_mut(&id).unwrap();
                            if channel.read_flag {
                                match channel.input.write_all(&payload).await {
                                    Ok(_) => {
                                        channel.input_processed += payload.len();
                                        if channel.input_processed > NOTIFY_AFTER_PROCESSED_SIZE {
                                            output_tx
                                                .send(Frame::ContentProcessed {
                                                    id: id,
                                                    processed: channel.input_processed as u32,
                                                })
                                                .await?;
                                                channel.input_processed = 0;
                                        }
                                    }
                                    Err(_) => {
                                        output_tx.send(Frame::ReadComplited { id }).await?;
                                        channel.read_flag = false;
                                    }
                                }
                            }
                        },
                        Frame::ContentProcessed { id, processed } => {
                            let channel = map.get_mut(&id).unwrap();
                            channel.output_w_size
                                .fetch_add(processed as usize, std::sync::atomic::Ordering::Relaxed);
                        },
                        Frame::WriteComplited { id } => {
                            let channel = map.get_mut(&id).unwrap();
                            channel.rem_write_flag = false;
                            if channel.read_flag {
                                channel.in_arbiter.close();
                                output_tx.send(Frame::ReadComplited { id }).await?;
                                channel.read_flag = false;
                            }
                            if !channel.rem_write_flag && !channel.rem_read_flag {
                                map.remove(&id).unwrap();
                                if let ChannelId::Local(id) = id {
                                    ids_pool.return_id(id).unwrap();
                                }
                            }
                        },
                        Frame::ReadComplited { id } => {
                            let channel = map.get_mut(&id).unwrap();
                            channel.rem_read_flag = false;
                            channel.out_arbiter.close();
                            if !channel.rem_write_flag && !channel.rem_read_flag {
                                map.remove(&id).unwrap();
                                if let ChannelId::Local(id) = id {
                                    ids_pool.return_id(id).unwrap();
                                }
                            }
                        },
                    },
                    None => break,
                }
            }
        };
    }

    Ok(())
}

fn spawn_channel_read(
    id: ChannelId,
    mut c_output: PipeReader,
    input: Sender<Frame>,
    c_output_w_size: Arc<AtomicUsize>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = async move {
            let mut buf = [0_u8; 2 * 1024];
            loop {
                let size = c_output_w_size.load(std::sync::atomic::Ordering::Relaxed);
                let size = cmp::min(size as usize, buf.len());
                if size <= 0 {
                    if c_output.is_closed() {
                        break;
                    }
                    tokio::task::yield_now().await;
                    continue;
                }
                match c_output.read(&mut buf[0..size]).await {
                    Ok(n) if n > 0 => {
                        input
                            .send(Frame::Content {
                                id,
                                payload: buf[0..n].to_vec(),
                            })
                            .await?;
                        c_output_w_size.fetch_sub(n, std::sync::atomic::Ordering::Relaxed);
                    }
                    _ => break,
                }
            }
            input.send(Frame::WriteComplited { id }).await?;
            Result::<(), Box<dyn Error>>::Ok(())
        }
        .await
        {
            log::trace!("mux {} read loop end with error: {}", id, e);
        }
    })
}

const MAX_PAYLOAD_SIZE: usize = 20 * 1024;
const WIN_SIZE: usize = 5 * MAX_PAYLOAD_SIZE;
const NOTIFY_AFTER_PROCESSED_SIZE: usize = WIN_SIZE / 2;

struct ChannelRef {
    in_arbiter: PipeArbiter,
    out_arbiter: PipeArbiter,
    input: PipeWriter,
    input_processed: usize,
    output_w_size: Arc<AtomicUsize>,
    read_flag: bool,
    rem_read_flag: bool,
    rem_write_flag: bool,
}

#[derive(Debug)]
struct Channel {
    id: ChannelId,
    wo: PipeWriter,
    ri: PipeReader,
}
