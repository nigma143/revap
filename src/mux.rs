use std::{
    cmp,
    collections::HashMap,
    error::Error,
    io,
    sync::{atomic::AtomicUsize, Arc},
};

use id_pool::IdPool;
use log::{debug, info};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    select,
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
};

use crate::pipe::{self, PipeArbiter, PipeReader, PipeWriter};

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

async fn write_frame<W: AsyncWrite + Unpin>(mut write: W, value: Frame) -> io::Result<()> {
    match value {
        Frame::New { id, rem_w_size } => {
            let mut buf = [0; 9];
            buf[0] = 0x01;
            buf[1..5].copy_from_slice(&Into::<i32>::into(id).to_be_bytes()[..]);
            buf[5..9].copy_from_slice(&rem_w_size.to_be_bytes()[..]);
            write.write_all(&buf).await?;
        }
        Frame::Content { id, payload } => {
            if payload.len() > MAX_PAYLOAD_SIZE {
                return Err(io::Error::new(io::ErrorKind::Other, "payload too large"));
            }
            let mut buf = [0; 9];
            buf[0] = 0x02;
            buf[1..5].copy_from_slice(&Into::<i32>::into(id).to_be_bytes()[..]);
            buf[5..9].copy_from_slice(&(payload.len() as u32).to_be_bytes()[..]);
            write.write_all(&buf).await?;
            write.write_all(&payload).await?;
        }
        Frame::ContentProcessed { id, processed } => {
            let mut buf = [0; 9];
            buf[0] = 0x03;
            buf[1..5].copy_from_slice(&Into::<i32>::into(id).to_be_bytes()[..]);
            buf[5..9].copy_from_slice(&processed.to_be_bytes()[..]);
            write.write_all(&buf).await?;
        }
        Frame::WriteComplited { id } => {
            let mut buf = [0; 5];
            buf[0] = 0x04;
            buf[1..5].copy_from_slice(&Into::<i32>::into(id).to_be_bytes()[..]);
            write.write_all(&buf).await?;
        }
        Frame::ReadComplited { id } => {
            let mut buf = [0; 5];
            buf[0] = 0x05;
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
        0x01 => {
            let rem_w_size = read.read_u32().await?;
            Frame::New { id, rem_w_size }
        }
        0x02 => {
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
        0x03 => {
            let processed = read.read_u32().await?;
            Frame::ContentProcessed { id, processed }
        }
        0x04 => Frame::WriteComplited { id },
        0x05 => Frame::ReadComplited { id },
        _ => Err(io::Error::new(
            io::ErrorKind::Other,
            format!("opcode `{}` not expected", opcode),
        ))?,
    };

    Ok(frame)
}

pub struct MuxConnection {
    listen: Receiver<ChannelRx>,
    new: Sender<oneshot::Sender<ChannelRx>>,
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
                debug!("connection loop end with error: {}", e);
            }
        });

        Self {
            listen: listen_rx,
            new: new_tx,
        }
    }

    pub async fn accept(&mut self) -> io::Result<(ChannelId, PipeReader, PipeWriter)> {
        let channel = self
            .listen
            .recv()
            .await
            .ok_or(io_err("can't accept channel because transport is die"))?;

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
    new: Sender<oneshot::Sender<ChannelRx>>,
}

impl MuxConnector {
    pub async fn connect(&mut self) -> io::Result<(ChannelId, PipeReader, PipeWriter)> {
        let (in_tx, in_rx) = oneshot::channel();

        self.new
            .send(in_tx)
            .await
            .map_err(|_| io_err("can't create channel because transport is die"))?;

        let channel = in_rx
            .await
            .map_err(|_| io_err("error at connecting because transport is die"))?;

        Ok((channel.id, channel.ri, channel.wo))
    }
}

async fn connection_loop<R, W>(
    mut write: W,
    mut read: R,
    listen: Sender<ChannelRx>,
    mut new: Receiver<oneshot::Sender<ChannelRx>>,
) -> Result<(), Box<dyn Error>>
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    let mut map = HashMap::new();
    let mut ids_pool = IdPool::new();

    let (w_frame_tx, mut w_frame_rx) = mpsc::channel(1024);
    tokio::spawn(async move {
        if let Err(e) = async move {
            loop {
                match w_frame_rx.recv().await {
                    Some(frame) => {
                        debug!("<- {}", frame);
                        write_frame(&mut write, frame).await?;
                    }
                    None => break,
                }
            }
            Result::<(), Box<dyn Error>>::Ok(())
        }
        .await
        {
            debug!("transport write loop end with error: {}", e);
        }
    });

    let (r_frame_tx, mut r_frame_rx) = mpsc::channel(1024);
    tokio::spawn(async move {
        if let Err(e) = async move {
            loop {
                let frame = read_frame(&mut read).await?;
                debug!("-> {}", frame);
                r_frame_tx.send(frame).await?;
            }
            Result::<(), Box<dyn Error>>::Ok(())
        }
        .await
        {
            debug!("transport read loop end with error: {}", e);
        }
    });

    fn crate_channel(
        id: ChannelId,
        w_size: usize,
        rem_w_size: Arc<AtomicUsize>,
    ) -> (ChannelTx, PipeReader, ChannelRx) {
        let (ai, wi, ri) = pipe::new_pipe2(w_size);
        let (ao, wo, ro) = pipe::new_pipe2(w_size);
        (
            ChannelTx {
                id,
                input_arbiter: ai,
                output_arbiter: ao,
                mux_writer: wi,
                mux_writer_processed: 0,
                rem_w_size: rem_w_size.clone(),
                read_flag: true,
                rem_write_flag: true,
                rem_read_flag: true,
            },
            ro,
            ChannelRx { id, wo, ri },
        )
    }

    loop {
        select! {
            res = new.recv() => {
                match res {
                    Some(shot) => {
                        let id = ChannelId::Local(ids_pool.request_id().unwrap());
                        let rem_w_size = Arc::new(AtomicUsize::new(REM_WIN_SIZE));
                        let (channel_tx, ro, channel_rx) = crate_channel(id, REM_WIN_SIZE, rem_w_size.clone());
                        if let Ok(_) = shot.send(channel_rx) {
                            spawn_mux_read(id, rem_w_size.clone(), ro, w_frame_tx.clone()); //
                            map.insert(id, channel_tx);
                            w_frame_tx
                                .send(Frame::New {
                                    id,
                                    rem_w_size: REM_WIN_SIZE as u32,
                                })
                                .await?;
                        }
                    }
                    None => break,
                }
            },
            res = r_frame_rx.recv() => {
                match res {
                    Some(frame) => match frame {
                        Frame::New { id, rem_w_size } => {
                            let rem_w_size = Arc::new(AtomicUsize::new(rem_w_size as usize));
                            let (channel_tx, ro, channel_rx) = crate_channel(id, REM_WIN_SIZE, rem_w_size.clone());
                            spawn_mux_read(id, rem_w_size.clone(), ro, w_frame_tx.clone());
                            listen.send(channel_rx).await?;
                            map.insert(id, channel_tx);
                        }
                        Frame::Content { id, payload } => match map.get_mut(&id) {
                            Some(ChannelTx {
                                id,
                                mux_writer,
                                mux_writer_processed,
                                read_flag,
                                ..
                            }) if *read_flag => match mux_writer.write_all(&payload).await {
                                Ok(_) => {
                                    *mux_writer_processed += payload.len();
                                    if *mux_writer_processed > NOTIFY_AFTER_PROCESSED_SIZE {
                                        w_frame_tx
                                            .send(Frame::ContentProcessed {
                                                id: *id,
                                                processed: *mux_writer_processed as u32,
                                            })
                                            .await?;
                                        *mux_writer_processed = 0;
                                    }
                                }
                                Err(_) => {
                                    w_frame_tx.send(Frame::ReadComplited { id: *id }).await?;
                                    *read_flag = false;
                                }
                            },
                            _ => {}
                        },
                        Frame::ContentProcessed { id, processed } => match map.get_mut(&id) {
                            Some(c) => {
                                c.rem_w_size
                                    .fetch_add(processed as usize, std::sync::atomic::Ordering::Relaxed);
                            }
                            None => info!("id: {} not found", id),
                        },
                        Frame::WriteComplited { id } => match map.get_mut(&id) {
                            Some(channel) => {
                                channel.rem_write_flag = false;
                                if channel.read_flag {
                                    channel.input_arbiter.close();
                                    w_frame_tx.send(Frame::ReadComplited { id }).await?;
                                    channel.read_flag = false;
                                }
                                if !channel.rem_write_flag && !channel.rem_read_flag {
                                    map.remove(&id).unwrap();
                                    if let ChannelId::Local(id) = id {
                                        ids_pool.return_id(id).unwrap();
                                    }
                                }
                            }
                            None => info!("id: {} not found", id),
                        },
                        Frame::ReadComplited { id } => match map.get_mut(&id) {
                            Some(channel) => {
                                channel.rem_read_flag = false;
                                channel.output_arbiter.close();
                                if !channel.rem_write_flag && !channel.rem_read_flag {
                                    map.remove(&id).unwrap();
                                    if let ChannelId::Local(id) = id {
                                        ids_pool.return_id(id).unwrap();
                                    }
                                }
                            }
                            None => info!("id: {} not found", id),
                        },
                    },
                    None => break,
                }
            }
        };
    }

    Ok(())
}

fn spawn_mux_read(
    id: ChannelId,
    reader_w_size: Arc<AtomicUsize>,
    mut mux_reader: PipeReader,
    writer: Sender<Frame>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = async move {
            let mut buf = [0_u8; 2 * 1024];
            loop {
                let size = reader_w_size.load(std::sync::atomic::Ordering::Relaxed);
                let size = cmp::min(size as usize, buf.len());
                if size <= 0 {
                    if mux_reader.is_closed() {
                        break;
                    }
                    tokio::task::yield_now().await;
                    continue;
                }
                match mux_reader.read(&mut buf[0..size]).await {
                    Ok(n) if n > 0 => {
                        writer
                            .send(Frame::Content {
                                id,
                                payload: buf[0..n].to_vec(),
                            })
                            .await?;
                        reader_w_size.fetch_sub(n, std::sync::atomic::Ordering::Relaxed);
                    }
                    _ => break,
                }
            }
            writer.send(Frame::WriteComplited { id }).await?;
            Result::<(), Box<dyn Error>>::Ok(())
        }
        .await
        {
            debug!("mux {} read loop end with error: {}", id, e);
        }
    })
}

const MAX_PAYLOAD_SIZE: usize = 20 * 1024;
const REM_WIN_SIZE: usize = 5 * MAX_PAYLOAD_SIZE;
const NOTIFY_AFTER_PROCESSED_SIZE: usize = REM_WIN_SIZE / 2;

struct ChannelTx {
    id: ChannelId,
    input_arbiter: PipeArbiter,
    output_arbiter: PipeArbiter,
    mux_writer: PipeWriter,
    mux_writer_processed: usize,
    rem_w_size: Arc<AtomicUsize>,
    read_flag: bool,
    rem_read_flag: bool,
    rem_write_flag: bool,
}

#[derive(Debug)]
struct ChannelRx {
    id: ChannelId,
    wo: PipeWriter,
    ri: PipeReader,
}

fn io_err(detail: &str) -> io::Error {
    io::Error::new(io::ErrorKind::Other, detail)
}
