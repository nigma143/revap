use std::{io, net::SocketAddr};

use log::{debug, error, info};
use tokio::{io::split, net::TcpListener};

use crate::outbound::{Incoming, Outbound};

pub async fn inbound_tcp(addr: SocketAddr, mut outbounds: Vec<Outbound>) -> io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("tcp listen at {}", addr);

    let mut index = 0;
    while let Ok((stream, remoute_addr)) = listener.accept().await {
        debug!("tcp connection from {}", remoute_addr);

        if index >= outbounds.len() {
            index = 0;
        }
        let mut outbound = outbounds.get_mut(index).unwrap().clone();
        index += 1;

        tokio::spawn(async move {
            debug!("forwarding tcp({}/{}) --> {}", addr, remoute_addr, outbound);
            let (reader, writer) = split(stream);
            let r = outbound
                .forwarding(Incoming::Tcp {
                    remoute_addr,
                    reader,
                    writer,
                })
                .await;
            if let Err(e) = r {
                error!(
                    "error on process tcp({}/{}) --> {}. detail: {}",
                    addr, remoute_addr, outbound, e
                );
            }
        });
    }

    Ok(())
}
