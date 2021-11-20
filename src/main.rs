use std::{env, error::Error, net::SocketAddr, path::Path};

use bound::Inbound;
use id_pool::IdPool;
use tokio::io;
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
    let s = "
in:
  dsgfsd:     
    proto: tcp       
    listen: 127.0.0.1:8001
    write:
      - upstream1
    
out:
  upstream1:
    proto: revtcp
    endpoint: 127.0.0.1:5001
    access-keys: 
      - AGENT_KEY1
      - AGENT_KEY2";

    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let config_path = std::env::args().nth(1).unwrap_or("revap.yml".into());
    let config_content = std::fs::read_to_string(config_path)?;

    let config = YamlLoader::load_from_str(&config_content).unwrap();
    let section = &config[0];

    let inbounds = create_inbounds(&section["in"]).await;
    let outbounds = create_outbounds(&section["out"]).await;

    let mut handles = vec![];
    for (mut inbound, write) in inbounds {
        let outbounds: Vec<Outbound> = outbounds
            .iter()
            .filter(|x| write.iter().any(|y| y == x.alias()))
            .map(|x| x.clone())
            .collect();

        let handle = tokio::spawn(async move {
            let res = inbound.forwarding(outbounds).await;
            if let Err(e) = res {
                log::debug!("forwarding error: {}", e);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    Ok(())
}

async fn create_inbounds(section: &Yaml) -> Vec<(Inbound, Vec<String>)> {
    let mut inbounds = vec![];
    for o in section.as_hash() {
        for (alias, opts) in o.iter() {
            let alias = alias.as_str().unwrap();
            let proto = yaml_as_str(opts, "proto").unwrap();
            let write = yaml_as_vec_str(opts, "write").unwrap();
            let inbound = match proto {
                "tcp" => {
                    let listen = yaml_as_str(opts, "listen").unwrap().parse().unwrap();
                    Inbound::Tcp(TcpInbound::bind_tcp(alias, listen).await.unwrap())
                }
                "tls" => {
                    let listen = yaml_as_str(opts, "listen").unwrap().parse().unwrap();
                    let cert_chain_path = yaml_as_str(opts, "cert-chain").unwrap();
                    let key_der_path = yaml_as_str(opts, "private-key").unwrap();
                    let cert_chain = load_certs(Path::new(cert_chain_path)).unwrap();
                    let key_der = load_keys(Path::new(key_der_path)).unwrap().remove(0);
                    Inbound::Tcp(
                        TcpInbound::bind_tls(alias, listen, cert_chain, key_der)
                            .await
                            .unwrap(),
                    )
                }
                "revtcp" => {
                    let endpoint = yaml_as_str(opts, "endpoint").unwrap().parse().unwrap();
                    let access_key = yaml_as_str(opts, "access-key").unwrap();
                    Inbound::RevTcp(RevTcpInbound::bind_tcp(alias, endpoint, access_key))
                }
                "revtls" => {
                    let endpoint = yaml_as_str(opts, "endpoint").unwrap().parse().unwrap();
                    let access_key = yaml_as_str(opts, "access-key").unwrap();
                    Inbound::RevTcp(RevTcpInbound::bind_tls(alias, endpoint, access_key))
                }
                _ => panic!("protocol `{}` not supported", proto),
            };
            inbounds.push((inbound, write));
        }
    }
    inbounds
}

async fn create_outbounds(section: &Yaml) -> Vec<Outbound> {
    let mut outbounds = vec![];
    for o in section.as_hash() {
        for (alias, opts) in o.iter() {
            let alias = alias.as_str().unwrap();
            let proto = yaml_as_str(opts, "proto").unwrap();
            let outbound = match proto {
                "tcp" => {
                    let endpoint = yaml_as_str(opts, "endpoint").unwrap().parse().unwrap();
                    Outbound::Tcp(TcpOutbound::new_tcp(alias, endpoint))
                }
                "tls" => {
                    let endpoint = yaml_as_str(opts, "endpoint").unwrap().parse().unwrap();
                    Outbound::Tcp(TcpOutbound::new_tls(alias, endpoint))
                }
                "revtcp" => {
                    let listen = yaml_as_str(opts, "listen").unwrap().parse().unwrap();
                    let access_keys = yaml_as_vec_str(opts, "access-keys").unwrap();
                    Outbound::RevTcp(
                        RevTcpOutbound::bind_tcp(alias, listen, access_keys)
                            .await
                            .unwrap(),
                    )
                }
                "revtls" => {
                    let listen = yaml_as_str(opts, "listen").unwrap().parse().unwrap();
                    let access_keys = yaml_as_vec_str(opts, "access-keys").unwrap();
                    let cert_chain_path = yaml_as_str(opts, "cert-chain").unwrap();
                    let key_der_path = yaml_as_str(opts, "private-key").unwrap();
                    let cert_chain = load_certs(Path::new(cert_chain_path)).unwrap();
                    let key_der = load_keys(Path::new(key_der_path)).unwrap().remove(0);
                    Outbound::RevTcp(
                        RevTcpOutbound::bind_tls(alias, listen, access_keys, cert_chain, key_der)
                            .await
                            .unwrap(),
                    )
                }
                _ => panic!("protocol `{}` not supported", proto),
            };
            outbounds.push(outbound);
        }
    }
    outbounds
}

fn yaml_as_str<'a>(node: &'a Yaml, name: &str) -> Result<&'a str, Box<dyn Error>> {
    match &node[name] {
        Yaml::BadValue => Err(format!("key `{}` not found. Node: `{:?}`", name, node).into()),
        val => val
            .as_str()
            .ok_or(format!("invalid value as str by key `{}`. Node: `{:?}`", name, node).into()),
    }
}

fn yaml_as_vec_str<'a>(node: &'a Yaml, name: &str) -> Result<Vec<String>, Box<dyn Error>> {
    match &node[name] {
        Yaml::BadValue => Err(format!("key `{}` not found. Node: `{:?}`", name, node).into()),
        val => match val.as_vec() {
            Some(val) => {
                let mut res = vec![];
                for e in val.iter() {
                    let e: Result<_, Box<dyn Error>> = e.as_str().ok_or(
                        format!("invalid value as str by key `{}`. Node: `{:?}`", name, node)
                            .into(),
                    );
                    res.push(e?.into());
                }
                Ok(res)
            }
            None => Err(format!("key `{}` not array. Node: `{:?}`", name, node).into()),
        },
    }
}

fn load_certs(path: &Path) -> Result<Vec<rustls::Certificate>, Box<dyn Error>> {
    let file = std::fs::File::open(path)
        .map_err(|e| format!("problem opening the file: `{:?}`. details: {:?}", path, e))?;
    rustls_pemfile::certs(&mut std::io::BufReader::new(file))
        .map_err(|e| {
            format!(
                "problem load certs from file: `{:?}`. details: {:?}",
                path, e
            )
            .into()
        })
        .map(|mut certs| certs.drain(..).map(rustls::Certificate).collect())
}

fn load_keys(path: &Path) -> Result<Vec<rustls::PrivateKey>, Box<dyn Error>> {
    let file = std::fs::File::open(path)
        .map_err(|e| format!("problem opening the file: `{:?}`. details: {:?}", path, e))?;
    rustls_pemfile::rsa_private_keys(&mut std::io::BufReader::new(file))
        .map_err(|e| {
            format!(
                "problem load private key from file: `{:?}`. details: {:?}",
                path, e
            )
            .into()
        })
        .map(|mut keys| keys.drain(..).map(rustls::PrivateKey).collect())
}
