use std::{error::Error, net::SocketAddr};

use warp::{
    hyper::{Response, StatusCode},
    Filter,
};
use yaml_rust::Yaml;

use crate::{domain::Domain, yaml_as_str, yaml_as_vec_str};

pub fn run(domain: Domain, config: &Yaml) -> Result<(), Box<dyn Error>> {
    let listen: SocketAddr = yaml_as_str(config, "listen")?.parse()?;
    let access_keys = yaml_as_vec_str(config, "access-keys")?;
    let stats = warp::get()
        .and(warp::path("stats"))
        .and(warp::query::raw())
        .map(move |access_key: String| {
            let grant = access_keys.contains(&access_key);
            if !grant {
                Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .body("wrong access key".into())
            } else {
                Response::builder().body(format!("{:#?}", domain))
            }
        });

    tokio::spawn(async move {
        warp::serve(stats).run(listen).await;
    });

    Ok(())
}
