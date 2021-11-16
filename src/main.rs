use std::{env, net::SocketAddr, path::Path};

use id_pool::IdPool;
use tokio::io;

use crate::{
    outbound::Outbound,
    revtcp_bound::{RevTcpInbound, RevTcpOutbound},
    tcp_bound::{TcpInbound, TcpOutbound},
};

pub mod mux;
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
        let outbounds = vec![Outbound::Tcp(
            "tls_test".into(),
            TcpOutbound::new_tls("sdfds", "127.0.0.1:8080".to_string().parse().unwrap()),
        )];

        let mut inbound = RevTcpInbound::bind_tls("rev_tls_test", addr1, "hello".into());
        inbound.forwarding(outbounds).await?;
        //inbound_tcp(addr1, outbounds).await?;
    } else {
        let addr1 = SocketAddr::from(([0, 0, 0, 0], 5001));

        let cert_chain = load_certs(Path::new("testdata/cert.pem"))?;
        let key_der = load_keys(Path::new("testdata/key.pem"))?.remove(0);

        let rev = RevTcpOutbound::bind_tls(
            SocketAddr::from(([127, 0, 0, 1], 4001)),
            vec!["hello".into()],
            cert_chain,
            key_der,
        )
        .await
        .unwrap();
        let outbounds = vec![Outbound::RevTcp("rev_tls_test".into(), rev)];

        //let rev = TcpOutbound::new(SocketAddr::from(([127, 0, 0, 1], 4001)));
        //let outbounds = vec![Outbound::Tcp(rev)];

        //inbound_tls(addr1, Path::new("testdata/cert.pem"), Path::new("testdata/key.pem"), outbounds).await?;
        let mut inbound = TcpInbound::bind_tcp("tcp in alias", addr1).await.unwrap();
        inbound.forwarding(outbounds).await?;
    }

    Ok(())
}

fn load_certs(path: &Path) -> io::Result<Vec<rustls::Certificate>> {
    rustls_pemfile::certs(&mut std::io::BufReader::new(std::fs::File::open(path)?))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid cert"))
        .map(|mut certs| certs.drain(..).map(rustls::Certificate).collect())
}

fn load_keys(path: &Path) -> io::Result<Vec<rustls::PrivateKey>> {
    rustls_pemfile::rsa_private_keys(&mut std::io::BufReader::new(std::fs::File::open(path)?))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid key"))
        .map(|mut keys| keys.drain(..).map(rustls::PrivateKey).collect())
}
