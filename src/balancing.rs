use std::{collections::HashMap, sync::Mutex};

use weighted_rs::{SmoothWeight, Weight};
use wildmatch::WildMatch;

use crate::bound::Outbound;

pub struct WildMatchGroup {
    groups: Vec<(WildMatch, LoadBalancing)>,
}

impl WildMatchGroup {
    pub fn new(groups: Vec<(&str, LoadBalancing)>) -> Self {
        Self {
            groups: groups
                .into_iter()
                .map(|(x, y)| (WildMatch::new(x), y))
                .collect(),
        }
    }

    pub fn select(&self, val: &str) -> Option<Outbound> {
        self.groups
            .iter()
            .find(|(x, _)| x.matches(val))
            .map(|x| &x.1)
            .and_then(|x| Some(x.select()))
    }
}

pub enum LoadBalancing {
    RoundRobin(RoundRobin),
    LeastConn(LeastConn),
}

impl LoadBalancing {
    pub fn round_robin(outbounds: Vec<Outbound>) -> Self {
        LoadBalancing::RoundRobin(RoundRobin::new(outbounds))
    }

    pub fn least_conn(outbounds: Vec<Outbound>) -> Self {
        LoadBalancing::LeastConn(LeastConn::new(outbounds))
    }

    pub fn select(&self) -> Outbound {
        match self {
            LoadBalancing::RoundRobin(o) => o.select(),
            LoadBalancing::LeastConn(o) => o.select(),
        }
    }
}

pub struct RoundRobin {
    weighted_index: Mutex<SmoothWeight<usize>>,
    outbounds: Vec<Outbound>,
}

impl RoundRobin {
    pub fn new(outbounds: Vec<Outbound>) -> Self {
        let mut weighted_index = SmoothWeight::new();
        for (pos, outbound) in outbounds.iter().enumerate() {
            weighted_index.add(pos, outbound.info().weight());
        }
        Self {
            weighted_index: Mutex::new(weighted_index),
            outbounds,
        }
    }

    pub fn select(&self) -> Outbound {
        let mut weighted_index = self.weighted_index.lock().unwrap();
        let pos = weighted_index.next().unwrap();
        self.outbounds.get(pos).unwrap().clone()
    }
}

pub struct LeastConn {
    outbounds: Vec<Outbound>,
}

impl LeastConn {
    pub fn new(outbounds: Vec<Outbound>) -> Self {
        Self { outbounds }
    }

    pub fn select(&self) -> Outbound {
        self.outbounds
            .iter()
            .min_by_key(|x| x.info().stats().active_conn() / x.info().weight() as usize)
            .unwrap()
            .clone()
    }
}
