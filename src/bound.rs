use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc::UnboundedSender,
};
use tracing::{error, info, info_span};

use crate::{
    balancing::LoadBalancing,
    revtcp_bound::{RevTcpInbound, RevTcpOutbound},
    tcp_bound::{TcpInbound, TcpOutbound},
};

pub struct Forwarder {
    pub alias: String,
    pub inbound: Inbound,
    gateways: LoadBalancing,
}

impl Forwarder {
    pub fn new(alias: String, inbound: Inbound, gateways: LoadBalancing) -> Self {
        Self {
            alias,
            inbound,
            gateways,
        }
    }

    pub async fn run(&mut self, shutdown: UnboundedSender<()>) {
        let _span = info_span!(
            "run",
            r#in = %self.alias,
            out = %self.gateways
                .iter()
                .map(|x| x.alias())
                .collect::<Vec<&str>>()
                .join(", ")
        );
        info!("started");
        let res = self.inbound.forwarding(&mut self.gateways).await;
        if let Err(e) = res {
            error!("error: {}", e)
        };
        let _ = shutdown.send(());
        info!("end");
    }
}

#[derive(Clone)]
pub struct Gateway {
    pub alias: String,
    pub outbound: Outbound,
    pub active_conn: Arc<AtomicUsize>,
}

impl Gateway {
    pub fn new(alias: String, outbound: Outbound) -> Self {
        Self {
            alias,
            outbound,
            active_conn: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }

    pub fn active_conn(&self) -> usize {
        self.active_conn.load(Ordering::Relaxed)
    }

    pub async fn forward<R, W>(&mut self, incoming: Incoming<R, W>) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
        R: AsyncRead + Unpin,
    {
        self.active_conn.fetch_add(1, Ordering::Relaxed);
        let res = self.outbound.forward(incoming).await;
        self.active_conn.fetch_sub(1, Ordering::Relaxed);
        res
    }
}

pub enum Inbound {
    Tcp(TcpInbound),
    RevTcp(RevTcpInbound),
}

impl Inbound {
    pub async fn forwarding(&mut self, gateways: &mut LoadBalancing) -> io::Result<()> {
        match self {
            Inbound::Tcp(o) => o.forwarding(gateways).await,
            Inbound::RevTcp(o) => o.forwarding(gateways).await,
        }
    }
}

pub struct Incoming<R, W> {
    pub reader: R,
    pub writer: W,
}

#[derive(Clone)]
pub enum Outbound {
    Tcp(TcpOutbound),
    RevTcp(RevTcpOutbound),
}

impl Outbound {
    pub async fn forward<R, W>(&mut self, incoming: Incoming<R, W>) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
        R: AsyncRead + Unpin,
    {
        match self {
            Outbound::Tcp(o) => o.forward(incoming).await,
            Outbound::RevTcp(o) => o.forward(incoming).await,
        }
    }
}

pub async fn io_process<R, W, OR, OW>(
    mut ir: R,
    mut iw: W,
    mut or: OR,
    mut ow: OW,
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
            let n = ir.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            ow.write_all(&buf[0..n]).await?;
        }
        //tokio::io::copy(&mut ir, &mut ow).await?;
        ow.shutdown().await
    };

    let server_to_client = async {
        let mut buf = vec![0; 2 * 1024].into_boxed_slice();
        loop {
            let n = or.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            iw.write_all(&buf[0..n]).await?;
        }
        //tokio::io::copy(&mut or, &mut iw).await?;
        iw.shutdown().await
    };

    tokio::try_join!(client_to_server, server_to_client)?;
    Ok(())
}
