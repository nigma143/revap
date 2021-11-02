use std::{
    cmp,
    collections::HashMap,
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicU32, AtomicUsize},
        Arc, Mutex,
    },
};

use id_pool::IdPool;
use log::*;
use tokio::{
    io::{split, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc::{self, error::TryRecvError, Receiver, Sender},
};

use crate::pipe::{self, PipeReader, PipeWriter};

pub type ChannelId = u32;

enum Frame {
    New { id: ChannelId, rem_w_size: u32 },
    Content { id: ChannelId, payload: Vec<u8> },
    ContentProcessed { id: ChannelId, remaining: u32 },
    Terminated { id: ChannelId },
}

async fn read_frame<R: AsyncRead + Unpin>(mut read: R) -> io::Result<Frame> {
    let opcode = read.read_u8().await?;
    let frame = match opcode {
        0x01 => {
            let id = read.read_u32().await?;
            let rem_w_size = read.read_u32().await?;
            Frame::New { id, rem_w_size }
        }
        0x02 => {
            let id = read.read_u32().await?;
            let len = read.read_u32().await? as usize;
            let mut payload = Vec::with_capacity(len);
            unsafe {
                payload.set_len(len);
            }
            read.read_exact(&mut payload).await?;
            Frame::Content { id, payload }
        }
        0x03 => {
            let id = read.read_u32().await?;
            let remaining = read.read_u32().await?;
            Frame::ContentProcessed { id, remaining }
        }
        0x04 => {
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

pub struct MuxStream {
    id: ChannelId,
    input: Pin<PipeReader>,
    output: PipeWriter,
}

pub struct PipeTerminator<T> {
    pipe: T,
    term: Arc<Terminator>
}

impl<T> PipeTerminator<T> {
    fn new(pipe: T, term: Arc<Terminator>) -> Self {
        Self {
            pipe,
            term
        }
    }
}

impl<T> std::ops::Deref for PipeTerminator<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.pipe
    }
}

impl<T> std::ops::DerefMut for PipeTerminator<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.pipe
    }
}

struct Terminator {
    id: ChannelId,
    message_tx: Sender<Frame>,
}

impl Terminator {
    fn new(id: ChannelId, message_tx: Sender<Frame>) -> Self {
        Self {
            id,
            message_tx
        }
    }
}

impl Drop for Terminator {
    fn drop(&mut self) {
        println!("sdfdsfdsfsfdsf!!!!!!!!!!!!!!!!!!!!!!");
        let _ = self.message_tx.send(Frame::Terminated { id: self.id });
    }
}

const DEFAULT_REM_WIN_SIZE: u32 = 5 * 20 * 1024;

#[derive(Clone)]
pub struct MuxConnector {
    message_tx: Sender<Frame>,
    connect_tx: Sender<Incoming>,
    ids_pool: Arc<Mutex<IdPool>>,
}

impl MuxConnector {
    pub fn new<S>(stream: S) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (read, write) = split(stream);

        let ids_pool = Arc::new(Mutex::new(IdPool::new()));
        let ids_pool_ref1 = ids_pool.clone();

        let (message_tx, message_rx) = mpsc::channel(1024);
        let message_tx_ref = message_tx.clone();

        let (connect_tx, connect_rx) = mpsc::channel(1024);

        tokio::spawn(async move { MuxConnector::write_loop(write, message_rx).await });
        tokio::spawn(async move {
            MuxConnector::read_loop(read, ids_pool_ref1, message_tx_ref, connect_rx).await
        });

        Self {
            message_tx,
            connect_tx,
            ids_pool,
        }
    }

    pub async fn connect(&mut self) -> io::Result<(ChannelId, PipeTerminator<PipeReader>, PipeTerminator<PipeWriter>)> {
        let id = self.ids_pool.lock().unwrap().request_id().unwrap();
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

        tokio::spawn(
            async move { MuxConnector::conn_loop(message_tx, id, read_out, w_size).await },
        );

        let term = Arc::new(Terminator::new(id, self.message_tx.clone()));

        let ri_term = PipeTerminator::new(read_in, term.clone());
        let wo_term = PipeTerminator::new(write_out, term);
        
        Ok((id, ri_term, wo_term))
    }

    pub fn is_closed(&self) -> bool {
        self.message_tx.is_closed() || self.connect_tx.is_closed()
    }

    async fn conn_loop(
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
                        /*message_tx
                        .send(Frame::Terminated { id })
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "message channel is die")
                        })?;*/
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
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "message channel is die")
                        })?;
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
            match message_rx.recv().await {
                Some(msg) => {
                    if log_enabled!(log::Level::Debug) {
                        match &msg {
                            Frame::New { id, rem_w_size } => {
                                debug!("tx new(id: {}, rem_w_size: {})", id, rem_w_size)
                            }
                            Frame::Content { id, payload } => {
                                debug!("tx content(id: {}, payload_len: {})", id, payload.len())
                            }
                            Frame::ContentProcessed { id, remaining } => {
                                debug!("tx processed(id: {}, remaining: {})", id, remaining)
                            }
                            Frame::Terminated { id } => debug!("tx terminated(id: {})", id),
                        }
                    }

                    write_frame(&mut write, msg).await?;
                }
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "message channel is die",
                    ))
                }
            }
        }
    }

    async fn read_loop<R>(
        mut read: R,
        ids_pool: Arc<Mutex<IdPool>>,
        message_tx: Sender<Frame>,
        mut connect_rx: Receiver<Incoming>,
    ) -> io::Result<()>
    where
        R: AsyncRead + Unpin,
    {
        let mut map = HashMap::new();

        loop {
            match connect_rx.try_recv() {
                Ok(c) => {
                    map.insert(c.id, c);
                }
                Err(e) => match e {
                    TryRecvError::Empty => {}
                    TryRecvError::Disconnected => Err(io::Error::new(
                        io::ErrorKind::Other,
                        "listen channel is closed",
                    ))?,
                },
            };
            match read_frame(&mut read).await? {
                Frame::Content { id, payload } => {
                    debug!("rx content(id: {}, len: {})", id, payload.len());
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
                                        io::Error::new(
                                            io::ErrorKind::Other,
                                            "message channel is die",
                                        )
                                    })?;
                            }
                            Err(_) => {
                                map.remove(&id).unwrap();
                                message_tx
                                    .send(Frame::Terminated { id })
                                    .await
                                    .map_err(|_| {
                                        io::Error::new(
                                            io::ErrorKind::Other,
                                            "message channel is die",
                                        )
                                    })?;
                            }
                        },
                        None => {}
                    }
                }
                Frame::ContentProcessed { id, remaining } => {
                    debug!("rx processed(id: {}, remaining: {})", id, remaining);
                    match map.get_mut(&id) {
                        Some(c) => {
                            c.rem_w_size
                                .store(remaining as usize, std::sync::atomic::Ordering::Relaxed);
                        }
                        None => {}
                    }
                }
                Frame::Terminated { id } => {
                    debug!("rx terminated(id: {})", id);
                    ids_pool.lock().unwrap().return_id(id).unwrap();
                    match map.remove(&id) {
                        Some(c) => drop(c),
                        None => {}
                    }
                }
                Frame::New { id, rem_w_size } => todo!(),
            };
        }
    }
}

pub struct MuxListener {
    message_tx: Sender<Frame>,
    listen_rx: Receiver<Outcoming>,
}

impl MuxListener {
    pub fn new<S>(stream: S) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (read, write) = split(stream);

        let (message_tx, message_rx) = mpsc::channel(1024);
        let message_tx_ref = message_tx.clone();

        let (listen_tx, listen_rx) = mpsc::channel(1024);

        tokio::spawn(async move { MuxListener::write_loop(write, message_rx).await });
        tokio::spawn(async move { MuxListener::read_loop(read, message_tx_ref, listen_tx).await });

        Self {
            message_tx,
            listen_rx,
        }
    }

    pub async fn listen(&mut self) -> io::Result<(ChannelId, PipeReader, PipeWriter)> {
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
            MuxListener::conn_loop(message_tx, outcoming.id, read_out, outcoming.rem_w_size).await
        });

        Ok((outcoming.id, outcoming.rx, write_out))
    }

    async fn conn_loop(
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
                        /*message_tx
                        .send(Frame::Terminated { id })
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "message channel is die")
                        })?;*/
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
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "message channel is die")
                        })?;
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
            match message_rx.recv().await {
                Some(msg) => {
                    if log_enabled!(log::Level::Debug) {
                        match &msg {
                            Frame::New { id, rem_w_size } => {
                                debug!("tx new(id: {}, rem_w_size: {})", id, rem_w_size)
                            }
                            Frame::Content { id, payload } => {
                                debug!("tx content(id: {}, payload_len: {})", id, payload.len())
                            }
                            Frame::ContentProcessed { id, remaining } => {
                                debug!("tx processed(id: {}, remaining: {})", id, remaining)
                            }
                            Frame::Terminated { id } => debug!("tx terminated(id: {})", id),
                        }
                    }

                    write_frame(&mut write, msg).await?;
                }
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "message channel is die",
                    ))
                }
            }
        }
    }

    async fn read_loop<R>(
        mut read: R,
        message_tx: Sender<Frame>,
        listen_tx: Sender<Outcoming>,
    ) -> io::Result<()>
    where
        R: AsyncRead + Unpin,
    {
        let mut map = HashMap::new();
        loop {
            match read_frame(&mut read).await? {
                Frame::New { id, rem_w_size } => {
                    debug!("rx new(id: {}, rem_w_size: {})", id, rem_w_size);
                    let (tx, rx) = pipe::new_pipe(rem_w_size as usize);
                    let rem_w_size = Arc::new(AtomicUsize::new(rem_w_size as usize));

                    listen_tx
                        .send(Outcoming {
                            id,
                            rx,
                            rem_w_size: rem_w_size.clone(),
                        })
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "listen channel is die")
                        })?;

                    map.insert(
                        id,
                        Incoming {
                            id,
                            tx,
                            rem_w_size: rem_w_size.clone(),
                        },
                    );
                }
                Frame::Content { id, payload } => {
                    debug!("rx content(id: {}, len: {})", id, payload.len());
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
                                        io::Error::new(
                                            io::ErrorKind::Other,
                                            "message channel is die",
                                        )
                                    })?;
                            }
                            Err(_) => {
                                map.remove(&id).unwrap();
                                message_tx
                                    .send(Frame::Terminated { id })
                                    .await
                                    .map_err(|_| {
                                        io::Error::new(
                                            io::ErrorKind::Other,
                                            "message channel is die",
                                        )
                                    })?;
                            }
                        },
                        None => {}
                    }
                }
                Frame::ContentProcessed { id, remaining } => {
                    debug!("rx processed(id: {}, remaining: {})", id, remaining);
                    match map.get_mut(&id) {
                        Some(c) => {
                            c.rem_w_size
                                .store(remaining as usize, std::sync::atomic::Ordering::Relaxed);
                        }
                        None => {}
                    }
                }
                Frame::Terminated { id } => {
                    debug!("rx terminated(id: {})", id);
                    match map.remove(&id) {
                        Some(c) => drop(c),
                        None => {}
                    }
                }
            }
        }
    }
}

pub struct Multiplexor {
    message_tx: Sender<Frame>,
    connect_tx: Sender<Incoming>,
    listen_rx: Receiver<Outcoming>,
    ids_pool: Arc<Mutex<IdPool>>,
}

impl Multiplexor {
    pub fn new<S>(stream: S) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (read, write) = split(stream);
        let ids_pool = Arc::new(Mutex::new(IdPool::new()));
        let ids_pool_ref1 = Arc::clone(&ids_pool);
        let ids_pool_ref2 = Arc::clone(&ids_pool);

        let (message_tx, message_rx) = mpsc::channel(1024);
        let message_tx_ref = message_tx.clone();

        let (connect_tx, connect_rx) = mpsc::channel(1024);
        let (listen_tx, listen_rx) = mpsc::channel(1024);

        tokio::spawn(async move { write_loop(write, ids_pool_ref1, message_rx).await });
        tokio::spawn(async move {
            read_loop(read, ids_pool_ref2, message_tx_ref, connect_rx, listen_tx).await
        });

        Self {
            message_tx,
            connect_tx,
            listen_rx,
            ids_pool,
        }
    }

    pub async fn listen(&mut self) -> io::Result<(ChannelId, PipeReader, PipeWriter)> {
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
            output_conn_loop(message_tx, outcoming.id, read_out, outcoming.rem_w_size).await
        });

        Ok((outcoming.id, outcoming.rx, write_out))
    }

    pub fn is_closed(&self) -> bool {
        self.message_tx.is_closed() || self.connect_tx.is_closed()
    }

    pub fn get_connector(&self) -> MultiplexorConnector {
        MultiplexorConnector {
            message_tx: self.message_tx.clone(),
            connect_tx: self.connect_tx.clone(),
            ids_pool: self.ids_pool.clone(),
        }
    }
}

pub struct MultiplexorConnector {
    message_tx: Sender<Frame>,
    connect_tx: Sender<Incoming>,
    ids_pool: Arc<Mutex<IdPool>>,
}

impl MultiplexorConnector {
    pub async fn connect(&mut self) -> io::Result<(ChannelId, PipeReader, PipeWriter)> {
        let id = self.ids_pool.lock().unwrap().request_id().unwrap() as u32;
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

        tokio::spawn(async move { output_conn_loop(message_tx, id, read_out, w_size).await });

        Ok((id, read_in, write_out))
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

async fn write_loop<W>(
    mut write: W,
    ids_pool: Arc<Mutex<IdPool>>,
    mut message_rx: Receiver<Frame>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    loop {
        match message_rx.recv().await {
            Some(msg) => {
                if log_enabled!(log::Level::Debug) {
                    match &msg {
                        Frame::New { id, rem_w_size } => {
                            debug!("tx new(id: {}, rem_w_size: {})", id, rem_w_size)
                        }
                        Frame::Content { id, payload } => {
                            debug!("tx content(id: {}, payload_len: {})", id, payload.len())
                        }
                        Frame::ContentProcessed { id, remaining } => {
                            debug!("tx processed(id: {}, remaining: {})", id, remaining)
                        }
                        Frame::Terminated { id } => debug!("tx terminated(id: {})", id),
                    }
                }

                if let Frame::Terminated { id } = msg {
                    //ids_pool.lock().unwrap().return_id(id);
                }

                write_frame(&mut write, msg).await?;
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "message channel is die",
                ))
            }
        }
    }
}

async fn read_loop<R>(
    mut read: R,
    ids_pool: Arc<Mutex<IdPool>>,
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
                debug!("rx new(id: {}, rem_w_size: {})", id, rem_w_size);
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
            Frame::Content { id, payload } => {
                debug!("rx content(id: {}, payload_len: {})", id, payload.len());
                loop {
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
                }
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
                    None => {}
                }
            }
            Frame::ContentProcessed { id, remaining } => {
                debug!("rx processed(id: {}, remaining: {})", id, remaining);
                match map.get_mut(&id) {
                    Some(c) => {
                        c.rem_w_size
                            .store(remaining as usize, std::sync::atomic::Ordering::Relaxed);
                    }
                    None => {}
                }
            }
            Frame::Terminated { id } => {
                debug!("rx terminated(id: {})", id);
                //ids_pool.lock().unwrap().return_id(id);
                match map.remove(&id) {
                    Some(c) => drop(c),
                    None => {}
                }
            }
        }
    }
}
