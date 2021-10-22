use std::net::SocketAddr;

use crate::{
    inbound::inbound_tcp,
    outbound::{Outbound, TcpOutbound, TlsOutbound},
};

mod inbound;
mod outbound;
mod pipe;
mod rev_tcp;

#[tokio::main()]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("debug"));

    let addr1 = SocketAddr::from(([0, 0, 0, 0], 5001));

    let outbounds = vec![
        Outbound::Tcp(TcpOutbound::new(
            "127.0.0.1:8080".to_string().parse().unwrap(),
        )),
        Outbound::Tls(TlsOutbound::new(
            "127.0.0.1:8080".to_string().parse().unwrap(),
        )),
    ];

    inbound_tcp(addr1, outbounds).await?;

    Ok(())
}
