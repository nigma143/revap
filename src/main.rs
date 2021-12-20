#![feature(drain_filter)]

use std::{collections::HashMap, error::Error, path::Path, sync::Arc};

use yaml_rust::{Yaml, YamlLoader};

use crate::{
    bound::{Inbound, Outbound},
    domain::{BalanceType, Domain, InboundInfo, OutboundInfo},
    revtcp_bound::{RevTcpInbound, RevTcpOutbound},
    tcp_bound::{TcpInbound, TcpOutbound},
};

mod balancing;
mod bound;
mod dashboard;
mod domain;
pub mod mux;
mod pipe;
mod revtcp_bound;
mod tcp_bound;

#[tokio::main()]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let config = {
        let config = std::env::args().nth(1).unwrap_or("revap.yml".into());
        let content = std::fs::read_to_string(&config)
            .map_err(|e| format!("config file {}. {}", config, e))?;
        YamlLoader::load_from_str(&content).unwrap()
    };
    let config = &config[0];
    
    let mut domain = Domain::default();

    let inbounds = create_inbounds(&mut domain, &config["in"]).await?;
    let outbounds = create_outbounds(&mut domain, &config["out"]).await?;

    for mut inbound in inbounds {
        let outbounds: Vec<Outbound> = outbounds
            .iter()
            .filter(|x| {
                inbound
                    .info()
                    .write_to()
                    .iter()
                    .any(|y| y == x.info().alias())
            })
            .map(|x| x.clone())
            .collect();

        tokio::spawn(async move { inbound.forwarding(outbounds).await });
    }

    match &config["dashboard"] {
        Yaml::BadValue => {}
        config => dashboard::run(domain.clone(), config)?,
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
    }

    Ok(())
}

async fn create_inbounds(
    domain: &mut Domain,
    section: &Yaml,
) -> Result<Vec<Inbound>, Box<dyn Error>> {
    let mut inbounds = vec![];
    for o in section.as_hash() {
        for (alias, opts) in o.iter() {
            let alias = alias.as_str().unwrap();
            let proto = yaml_as_str(opts, "proto")?;
            let balance = yaml_as_str(opts, "balance").unwrap_or("roundrobin");
            let balance = match balance {
                "roundrobin" => BalanceType::RoundRobin,
                "leastconn" => BalanceType::LeastConn,
                _ => return Err(format!("load balancing {} not supported", balance).into()),
            };
            let write_to = yaml_as_vec_str(opts, "write")?;
            let info = Arc::new(InboundInfo::new(alias, write_to, balance));
            let inbound = match proto {
                "tcp" => {
                    let listen = yaml_as_str(opts, "listen")?.parse()?;
                    Inbound::Tcp(TcpInbound::bind_tcp(info.clone(), listen).await?)
                }
                "tls" => {
                    let sni_map = y_select(&opts["sni"]).and_then(|x| Some(y_as_map_vec_str(x)));
                    let listen = yaml_as_str(opts, "listen")?.parse()?;
                    let cert_chain_path = yaml_as_str(opts, "cert-chain")?;
                    let key_der_path = yaml_as_str(opts, "private-key")?;
                    let cert_chain = load_certs(Path::new(cert_chain_path))?;
                    let key_der = load_keys(Path::new(key_der_path))?.remove(0);
                    Inbound::Tcp(
                        TcpInbound::bind_tls(info.clone(), listen, sni_map, cert_chain, key_der)
                            .await?,
                    )
                }
                "revtcp" => {
                    let endpoint = yaml_as_str(opts, "endpoint")?.parse()?;
                    let access_key = yaml_as_str(opts, "access-key")?;
                    Inbound::RevTcp(RevTcpInbound::bind_tcp(info.clone(), endpoint, access_key))
                }
                "revtls" => {
                    let endpoint = yaml_as_str(opts, "endpoint")?.parse()?;
                    let access_key = yaml_as_str(opts, "access-key")?;
                    Inbound::RevTcp(RevTcpInbound::bind_tls(info.clone(), endpoint, access_key))
                }
                _ => panic!("protocol `{}` not supported", proto),
            };
            domain.inbounds.push(info);
            inbounds.push(inbound);
        }
    }
    Ok(inbounds)
}

async fn create_outbounds(
    domain: &mut Domain,
    section: &Yaml,
) -> Result<Vec<Outbound>, Box<dyn Error>> {
    let mut outbounds = vec![];
    for o in section.as_hash() {
        for (alias, opts) in o.iter() {
            let alias = alias.as_str().unwrap();
            let proto = yaml_as_str(opts, "proto")?;
            let weight = yaml_as_isize(opts, "weight")?;
            let info = Arc::new(OutboundInfo::new(alias, weight));
            let outbound = match proto {
                "tcp" => {
                    let endpoint = yaml_as_str(opts, "endpoint")?.parse()?;
                    Outbound::Tcp(TcpOutbound::new_tcp(info.clone(), endpoint))
                }
                "tls" => {
                    let endpoint = yaml_as_str(opts, "endpoint")?.parse()?;
                    Outbound::Tcp(TcpOutbound::new_tls(info.clone(), endpoint))
                }
                "revtcp" => {
                    let listen = yaml_as_str(opts, "listen")?.parse()?;
                    let access_keys = yaml_as_vec_str(opts, "access-keys")?;
                    Outbound::RevTcp(
                        RevTcpOutbound::bind_tcp(info.clone(), listen, access_keys).await?,
                    )
                }
                "revtls" => {
                    let listen = yaml_as_str(opts, "listen")?.parse()?;
                    let access_keys = yaml_as_vec_str(opts, "access-keys")?;
                    let cert_chain_path = yaml_as_str(opts, "cert-chain")?;
                    let key_der_path = yaml_as_str(opts, "private-key")?;
                    let cert_chain = load_certs(Path::new(cert_chain_path))?;
                    let key_der = load_keys(Path::new(key_der_path))?.remove(0);
                    Outbound::RevTcp(
                        RevTcpOutbound::bind_tls(
                            info.clone(),
                            listen,
                            access_keys,
                            cert_chain,
                            key_der,
                        )
                        .await?,
                    )
                }
                _ => panic!("protocol `{}` not supported", proto),
            };
            domain.outbounds.push(info);
            outbounds.push(outbound);
        }
    }
    Ok(outbounds)
}

pub fn yaml_as_isize(node: &Yaml, name: &str) -> Result<isize, Box<dyn Error>> {
    match &node[name] {
        Yaml::BadValue => Err(format!("key {} not found. node: {:?}", name, node).into()),
        val => val
            .as_i64()
            .ok_or(format!("invalid value as str by key {}. node: {:?}", name, node).into())
            .map(|x| x as isize),
    }
}

pub fn yaml_as_str<'a>(node: &'a Yaml, name: &str) -> Result<&'a str, Box<dyn Error>> {
    match &node[name] {
        Yaml::BadValue => Err(format!("key {} not found. node: {:?}", name, node).into()),
        val => val
            .as_str()
            .ok_or(format!("invalid value as str by key {}. node: {:?}", name, node).into()),
    }
}

pub fn y_select(node: &Yaml) -> Option<&Yaml> {
    match node {
        Yaml::BadValue => None,
        _ => Some(node),
    }
}

pub fn y_as_map_vec_str(node: &Yaml) -> HashMap<String, Vec<String>> {
    match &node {
        Yaml::Hash(v) => {
            let mut map = HashMap::new();
            for val in v.iter() {
                let key = y_as_str(val.0);
                let value = y_as_vec_str(val.1);
                map.insert(key, value);
            }
            map
        }
        _ => panic!("invalid value as map vec str. node: {:?}", node),
    }
}

pub fn y_as_vec_str(node: &Yaml) -> Vec<String> {
    match &node {
        Yaml::Array(v) => v.iter().map(|x| y_as_str(x)).collect(),
        _ => panic!("invalid value as vec str. node: {:?}", node),
    }
}

pub fn y_as_str(node: &Yaml) -> String {
    match &node {
        Yaml::String(v) => v.into(),
        _ => panic!("invalid value as str. node: {:?}", node),
    }
}

pub fn yaml_as_vec_str(node: &Yaml, name: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let arr = yaml_as_vec(node, name)?;
    let mut res = vec![];
    for e in arr.iter() {
        let e: Result<_, Box<dyn Error>> = e
            .as_str()
            .ok_or(format!("invalid value as str by key `{}`. node: {:?}", name, node).into());
        res.push(e?.into());
    }
    Ok(res)
}

pub fn yaml_as_vec<'a>(node: &'a Yaml, name: &str) -> Result<&'a Vec<Yaml>, Box<dyn Error>> {
    match &node[name] {
        Yaml::BadValue => Err(format!("key `{}` not found. node: {:?}", name, node).into()),
        val => match val.as_vec() {
            Some(val) => Ok(val),
            None => Err(format!("key `{}` is not array. node: {:?}", name, node).into()),
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
