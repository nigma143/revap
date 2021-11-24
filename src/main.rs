use std::{error::Error, path::Path};

use bound::{Gateway, Inbound};
use tokio::sync::mpsc;
use yaml_rust::{Yaml, YamlLoader};

use crate::{
    balancing::LoadBalancing,
    bound::{Forwarder, Outbound},
    revtcp_bound::{RevTcpInbound, RevTcpOutbound},
    tcp_bound::{TcpInbound, TcpOutbound},
};

mod balancing;
mod bound;
pub mod mux;
mod pipe;
mod revtcp_bound;
mod tcp_bound;

#[tokio::main()]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let config = {
        let config = std::env::args().nth(1).unwrap_or("revap.yml".into());
        let content = std::fs::read_to_string(&config)
            .map_err(|e| format!("config file {}. {}", config, e))?;
        YamlLoader::load_from_str(&content).unwrap()
    };
    let config = &config[0];

    let forwarders = create_forwarders(&config).await?;

    let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel();

    for mut forwarder in forwarders {
        let shutdown = shutdown_tx.clone();
        tokio::spawn(async move { forwarder.run(shutdown).await });
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = shutdown_rx.recv() => {},
    }

    Ok(())
}

async fn create_forwarders(section: &Yaml) -> Result<Vec<Forwarder>, Box<dyn Error>> {
    let gateways = create_gateways(&section["out"]).await?;
    let section = &section["in"];
    let mut forwarders = vec![];
    for o in section.as_hash() {
        for (alias, opts) in o.iter() {
            let alias = alias.as_str().unwrap();
            let proto = yaml_as_str(opts, "proto")?;
            let balance = yaml_as_str(opts, "balance")?;
            let write = yaml_as_vec_str(opts, "write")?;
            let inbound = match proto {
                "tcp" => {
                    let listen = yaml_as_str(opts, "listen")?.parse()?;
                    Inbound::Tcp(TcpInbound::bind_tcp(listen).await?)
                }
                "tls" => {
                    let listen = yaml_as_str(opts, "listen")?.parse()?;
                    let cert_chain_path = yaml_as_str(opts, "cert-chain")?;
                    let key_der_path = yaml_as_str(opts, "private-key")?;
                    let cert_chain = load_certs(Path::new(cert_chain_path))?;
                    let key_der = load_keys(Path::new(key_der_path))?.remove(0);
                    Inbound::Tcp(TcpInbound::bind_tls(listen, cert_chain, key_der).await?)
                }
                "revtcp" => {
                    let endpoint = yaml_as_str(opts, "endpoint")?.parse()?;
                    let access_key = yaml_as_str(opts, "access-key")?;
                    Inbound::RevTcp(RevTcpInbound::bind_tcp(endpoint, access_key))
                }
                "revtls" => {
                    let endpoint = yaml_as_str(opts, "endpoint")?.parse()?;
                    let access_key = yaml_as_str(opts, "access-key")?;
                    Inbound::RevTcp(RevTcpInbound::bind_tls(endpoint, access_key))
                }
                _ => panic!("protocol `{}` not supported", proto),
            };
            let gateways: Vec<Gateway> = gateways
                .iter()
                .filter(|x| write.iter().any(|y| y == x.alias()))
                .map(|x| x.clone())
                .collect();
            let gateways = match balance {
                "roundrobin" => LoadBalancing::round_robin(gateways),
                "leastconn" => LoadBalancing::least_conn(gateways),
                _ => return Err(format!("load balancing {} not supported", balance).into()),
            };
            let forwarder = Forwarder::new(alias.into(), inbound, gateways);
            forwarders.push(forwarder);
        }
    }
    Ok(forwarders)
}

async fn create_gateways(section: &Yaml) -> Result<Vec<Gateway>, Box<dyn Error>> {
    let mut gateways = vec![];
    for o in section.as_hash() {
        for (alias, opts) in o.iter() {
            let alias = alias.as_str().unwrap();
            let proto = yaml_as_str(opts, "proto")?;
            let outbound = match proto {
                "tcp" => {
                    let endpoint = yaml_as_str(opts, "endpoint")?.parse()?;
                    Outbound::Tcp(TcpOutbound::new_tcp(endpoint))
                }
                "tls" => {
                    let endpoint = yaml_as_str(opts, "endpoint")?.parse()?;
                    Outbound::Tcp(TcpOutbound::new_tls(endpoint))
                }
                "revtcp" => {
                    let listen = yaml_as_str(opts, "listen")?.parse()?;
                    let access_keys = yaml_as_vec_str(opts, "access-keys")?;
                    Outbound::RevTcp(RevTcpOutbound::bind_tcp(listen, access_keys).await?)
                }
                "revtls" => {
                    let listen = yaml_as_str(opts, "listen")?.parse()?;
                    let access_keys = yaml_as_vec_str(opts, "access-keys")?;
                    let cert_chain_path = yaml_as_str(opts, "cert-chain")?;
                    let key_der_path = yaml_as_str(opts, "private-key")?;
                    let cert_chain = load_certs(Path::new(cert_chain_path))?;
                    let key_der = load_keys(Path::new(key_der_path))?.remove(0);
                    Outbound::RevTcp(
                        RevTcpOutbound::bind_tls(listen, access_keys, cert_chain, key_der).await?,
                    )
                }
                _ => panic!("protocol `{}` not supported", proto),
            };
            let gateway = Gateway::new(alias.into(), outbound);
            gateways.push(gateway);
        }
    }
    Ok(gateways)
}

fn yaml_as_str<'a>(node: &'a Yaml, name: &str) -> Result<&'a str, Box<dyn Error>> {
    match &node[name] {
        Yaml::BadValue => Err(format!("key {} not found. node: {:?}", name, node).into()),
        val => val
            .as_str()
            .ok_or(format!("invalid value as str by key {}. node: {:?}", name, node).into()),
    }
}

fn yaml_as_vec_str<'a>(node: &'a Yaml, name: &str) -> Result<Vec<String>, Box<dyn Error>> {
    match &node[name] {
        Yaml::BadValue => Err(format!("key {} not found. node: {:?}", name, node).into()),
        val => match val.as_vec() {
            Some(val) => {
                let mut res = vec![];
                for e in val.iter() {
                    let e: Result<_, Box<dyn Error>> = e.as_str().ok_or(
                        format!("invalid value as str by key {}. node: {:?}", name, node).into(),
                    );
                    res.push(e?.into());
                }
                Ok(res)
            }
            None => Err(format!("key {} is not array. node: {:?}", name, node).into()),
        },
    }
}

fn load_certs(path: &Path) -> Result<Vec<rustls::Certificate>, Box<dyn Error>> {
    let file = std::fs::File::open(path)
        .map_err(|e| format!("certificates file: {:?}. details: {}", path, e))?;
    rustls_pemfile::certs(&mut std::io::BufReader::new(file))
        .map_err(|e| format!("load certificates from file: `{:?}`. details: {}", path, e).into())
        .map(|mut certs| certs.drain(..).map(rustls::Certificate).collect())
}

fn load_keys(path: &Path) -> Result<Vec<rustls::PrivateKey>, Box<dyn Error>> {
    let file = std::fs::File::open(path)
        .map_err(|e| format!("private key file: {:?}. details: {}", path, e))?;
    rustls_pemfile::rsa_private_keys(&mut std::io::BufReader::new(file))
        .map_err(|e| format!("load private keys from file: `{:?}`. details: {}", path, e).into())
        .map(|mut keys| keys.drain(..).map(rustls::PrivateKey).collect())
}
