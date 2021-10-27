use std::{io};

use log::*;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
};

use crate::{
    revtcp_bound::RevTcpOutbound,
    tcp_bound::{TcpOutbound, TlsOutbound},
};

pub enum Incoming<R, W> {
    Stream {
        source: String,
        reader: R,
        writer: W,
    },
}

#[derive(Clone)]
pub enum Outbound {
    Tcp(TcpOutbound),
    Tls(TlsOutbound),
    RevTcp(RevTcpOutbound),
}

impl Outbound {
    pub async fn forwarding<R, W>(&mut self, incoming: Incoming<R, W>)
    where
        W: AsyncWrite + Unpin,
        R: AsyncRead + Unpin,
    {
        match self {
            Outbound::Tcp(o) => o.forwarding(incoming).await,
            Outbound::Tls(o) => o.forwarding(incoming).await,
            Outbound::RevTcp(o) => o.forwarding(incoming).await,
        };
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
        iw.shutdown().await
    };

    tokio::try_join!(client_to_server, server_to_client)?;
    Ok(())
}
