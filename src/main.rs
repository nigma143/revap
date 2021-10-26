use std::{env, net::SocketAddr};

use tokio::io;

use crate::{
    inbound::inbound_tcp,
    outbound::{Outbound, TcpOutbound, TlsOutbound},
    revtcp_bound::{inbound_revtcp, RevTcpOutbound},
};

mod inbound;
mod multiplexor;
mod outbound;
mod pipe;
mod revtcp_bound;

#[tokio::main()]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let args: Vec<String> = env::args().collect();
    println!("{:?}", args);
    if args.len() > 1 {
        let addr1 = SocketAddr::from(([127, 0, 0, 1], 4001));
        let outbounds = vec![Outbound::Tls(TlsOutbound::new(
            "127.0.0.1:8080".to_string().parse().unwrap(),
        ))];

        inbound_revtcp(addr1, outbounds).await?;
    } else {
        let addr1 = SocketAddr::from(([127, 0, 0, 1], 5001));

        let rev = RevTcpOutbound::bind(SocketAddr::from(([127, 0, 0, 1], 4001)));

        let outbounds = vec![Outbound::RevTcp(rev)];

        inbound_tcp(addr1, outbounds).await?;
    }

    Ok(())
}
