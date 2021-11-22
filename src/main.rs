use std::{error::Error, path::Path};

use bound::Inbound;
use tokio::sync::mpsc;
use tracing::{error, info, info_span, Instrument};
use yaml_rust::{Yaml, YamlLoader};

use crate::{
    bound::Outbound,
    revtcp_bound::{RevTcpInbound, RevTcpOutbound},
    tcp_bound::{TcpInbound, TcpOutbound},
};

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

    let inbounds = create_inbounds(&config["in"]).await?;
    let outbounds = create_outbounds(&config["out"]).await?;

    let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel();

    for (mut inbound, write) in inbounds {
        let mut outbounds: Vec<Outbound> = outbounds
            .iter()
            .filter(|x| write.iter().any(|y| y == x.alias()))
            .map(|x| x.clone())
            .collect();
        let shutdown = shutdown_tx.clone();

        let span = info_span!(
            "process",
            r#in = inbound.alias(),
            out = %outbounds
                .iter()
                .map(|x| x.alias())
                .collect::<Vec<&str>>()
                .join(", ")
        );

        tokio::spawn(
            async move {
                info!("started");
                let res = inbound.forwarding(&mut outbounds).await;
                if let Err(e) = res {
                    error!("error: {}", e)
                };
                let _ = shutdown.send(());
                info!("end");
            }
            .instrument(span),
        );
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = shutdown_rx.recv() => {},
    }

    Ok(())
}

async fn create_inbounds(section: &Yaml) -> Result<Vec<(Inbound, Vec<String>)>, Box<dyn Error>> {
    let mut inbounds = vec![];
    for o in section.as_hash() {
        for (alias, opts) in o.iter() {
            let alias = alias.as_str().unwrap();
            let proto = yaml_as_str(opts, "proto")?;
            let write = yaml_as_vec_str(opts, "write")?;
            let inbound = match proto {
                "tcp" => {
                    let listen = yaml_as_str(opts, "listen")?.parse()?;
                    Inbound::Tcp(TcpInbound::bind_tcp(alias, listen).await?)
                }
                "tls" => {
                    let listen = yaml_as_str(opts, "listen")?.parse()?;
                    let cert_chain_path = yaml_as_str(opts, "cert-chain")?;
                    let key_der_path = yaml_as_str(opts, "private-key")?;
                    let cert_chain = load_certs(Path::new(cert_chain_path))?;
                    let key_der = load_keys(Path::new(key_der_path))?.remove(0);
                    Inbound::Tcp(TcpInbound::bind_tls(alias, listen, cert_chain, key_der).await?)
                }
                "revtcp" => {
                    let endpoint = yaml_as_str(opts, "endpoint")?.parse()?;
                    let access_key = yaml_as_str(opts, "access-key")?;
                    Inbound::RevTcp(RevTcpInbound::bind_tcp(alias, endpoint, access_key))
                }
                "revtls" => {
                    let endpoint = yaml_as_str(opts, "endpoint")?.parse()?;
                    let access_key = yaml_as_str(opts, "access-key")?;
                    Inbound::RevTcp(RevTcpInbound::bind_tls(alias, endpoint, access_key))
                }
                _ => panic!("protocol `{}` not supported", proto),
            };
            inbounds.push((inbound, write));
        }
    }
    Ok(inbounds)
}

async fn create_outbounds(section: &Yaml) -> Result<Vec<Outbound>, Box<dyn Error>> {
    let mut outbounds = vec![];
    for o in section.as_hash() {
        for (alias, opts) in o.iter() {
            let alias = alias.as_str().unwrap();
            let proto = yaml_as_str(opts, "proto")?;
            let outbound = match proto {
                "tcp" => {
                    let endpoint = yaml_as_str(opts, "endpoint")?.parse()?;
                    Outbound::Tcp(TcpOutbound::new_tcp(alias, endpoint))
                }
                "tls" => {
                    let endpoint = yaml_as_str(opts, "endpoint")?.parse()?;
                    Outbound::Tcp(TcpOutbound::new_tls(alias, endpoint))
                }
                "revtcp" => {
                    let listen = yaml_as_str(opts, "listen")?.parse()?;
                    let access_keys = yaml_as_vec_str(opts, "access-keys")?;
                    Outbound::RevTcp(RevTcpOutbound::bind_tcp(alias, listen, access_keys).await?)
                }
                "revtls" => {
                    let listen = yaml_as_str(opts, "listen")?.parse()?;
                    let access_keys = yaml_as_vec_str(opts, "access-keys")?;
                    let cert_chain_path = yaml_as_str(opts, "cert-chain")?;
                    let key_der_path = yaml_as_str(opts, "private-key")?;
                    let cert_chain = load_certs(Path::new(cert_chain_path))?;
                    let key_der = load_keys(Path::new(key_der_path))?.remove(0);
                    Outbound::RevTcp(
                        RevTcpOutbound::bind_tls(alias, listen, access_keys, cert_chain, key_der)
                            .await?,
                    )
                }
                _ => panic!("protocol `{}` not supported", proto),
            };
            outbounds.push(outbound);
        }
    }
    Ok(outbounds)
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
