use std::{error::Error, net::SocketAddr};

use warp::{
    hyper::{Response, StatusCode},
    Filter,
};
use yaml_rust::Yaml;

use crate::{domain::Domain, y_as_str, y_as_vec_str};

pub fn run(domain: Domain, config: &Yaml) -> Result<(), Box<dyn Error>> {
    let listen: SocketAddr = y_as_str(&config["listen"]).parse().unwrap();
    let access_keys = y_as_vec_str(&config["access-keys"]);
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
                Response::builder().body(render_html(&domain))
            }
        });

    tokio::spawn(async move {
        warp::serve(stats).run(listen).await;
    });

    Ok(())
}

fn render_html(domain: &Domain) -> String {
    let mut html: String = r#"
    <html>
    <head>
        <title>Revap stats</title>
        <meta http-equiv="refresh" content="1">
    </head>
      <table border="1">  
          <caption>Inbounds</caption>
        <tr>
          <th>alias</th>
          <th>targets</th>
          <th>balance</th>
          <th>active-connection</th>
        </tr>
        "#
    .into();
    for inbound in &domain.inbounds {
        html.push_str("<tr>");
        html.push_str(&format!("<td>{}</td>", inbound.alias()));
        html.push_str(&format!("<td>{:?}</td>", inbound.write_to()));
        html.push_str(&format!("<td>{:?}</td>", inbound.balance_type()));
        html.push_str(&format!("<td>{}</td>", inbound.stats().active_conn()));
        html.push_str("</tr>");
    }
    html.push_str(
        r#"</table>
    <table border="1">  
        <caption>Outbounds</caption>
      <tr>
        <th>alias</th>
        <th>weight</th>
        <th>active-connection</th>
      </tr>"#,
    );
    for outbound in &domain.outbounds {
        html.push_str("<tr>");
        html.push_str(&format!("<td>{}</td>", outbound.alias()));
        html.push_str(&format!("<td>{}</td>", outbound.weight()));
        html.push_str(&format!("<td>{}</td>", outbound.stats().active_conn()));
        html.push_str("</tr>");
    }
    html.push_str(
        r#"</table>
    </html>"#,
    );
    html
}
