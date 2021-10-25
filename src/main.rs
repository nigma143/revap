use std::{env, net::SocketAddr};

use crate::{
    inbound::{inbound_revtcp, inbound_tcp},
    outbound::{Outbound, TcpOutbound, TlsOutbound},
    rev_tcp::RevTcpConnector,
};

mod inbound;
mod outbound;
mod pipe;
mod rev_tcp;

#[tokio::main()]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let args: Vec<String> = env::args().collect();
    println!("{:?}", args);
    if args.len() > 1 {
        let addr1 = SocketAddr::from(([127, 0, 0, 1], 4001));
        let outbounds = vec![Outbound::Tcp(TcpOutbound::new(
            "127.0.0.1:8080".to_string().parse().unwrap(),
        ))];

        inbound_revtcp(addr1, outbounds).await?;
    } else {
        let addr1 = SocketAddr::from(([127, 0, 0, 1], 5001));

        let rev = RevTcpConnector::bind(SocketAddr::from(([127, 0, 0, 1], 4001)));

        let outbounds = vec![Outbound::RevTcp(rev)];

        inbound_tcp(addr1, outbounds).await?;
    }

    Ok(())
}
