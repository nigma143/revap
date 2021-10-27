use std::{env, net::SocketAddr, path::Path};

use tokio::io;

use crate::{outbound::Outbound, revtcp_bound::{inbound_revtcp, RevTcpOutbound}, tcp_bound::{TlsOutbound, inbound_tcp, inbound_tls}};

mod multiplexor;
mod outbound;
mod pipe;
mod revtcp_bound;
mod tcp_bound;

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

        //inbound_tls(addr1, Path::new("testdata/cert.pem"), Path::new("testdata/key.pem"), outbounds).await?;
        inbound_tcp(addr1, outbounds).await?;
    }

    Ok(())
}
