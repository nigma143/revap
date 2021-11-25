use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{
    domain::{InboundInfo, OutboundInfo},
    revtcp_bound::{RevTcpInbound, RevTcpOutbound},
    tcp_bound::{TcpInbound, TcpOutbound},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub enum Inbound {
    Tcp(TcpInbound),
    RevTcp(RevTcpInbound),
}

impl Inbound {
    pub fn info(&self) -> &Arc<InboundInfo> {
        match self {
            Inbound::Tcp(o) => o.info(),
            Inbound::RevTcp(o) => o.info(),
        }
    }

    pub async fn forwarding(&mut self, outbounds: Vec<Outbound>) {
        match self {
            Inbound::Tcp(o) => o.forwarding(outbounds).await,
            Inbound::RevTcp(o) => o.forwarding(outbounds).await,
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
    pub fn info(&self) -> &Arc<OutboundInfo> {
        match self {
            Outbound::Tcp(o) => o.info(),
            Outbound::RevTcp(o) => o.info(),
        }
    }

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
