use std::collections::HashMap;

use warp::Filter;

use crate::domain::Domain;

pub async fn run_web(domain: Domain) {
    let stats = warp::get()
        .and(warp::path("stats"))
        .and(warp::path::param())
        .map(move |access_key: String| {
            format!("Hello {:?}, whose agent is. {:?}", access_key, domain)
        });
    warp::serve(stats)
        .run(([0, 0, 0, 0], 3030))
        .await  
}
