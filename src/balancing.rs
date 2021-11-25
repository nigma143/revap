use std::ops::Deref;

use crate::bound::Outbound;

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

    pub fn select(&mut self) -> &mut Outbound {
        match self {
            LoadBalancing::RoundRobin(o) => o.select(),
            LoadBalancing::LeastConn(o) => o.select(),
        }
    }
}

impl Deref for LoadBalancing {
    type Target = Vec<Outbound>;

    fn deref(&self) -> &Self::Target {
        match self {
            LoadBalancing::RoundRobin(o) => o,
            LoadBalancing::LeastConn(o) => o,
        }
    }
}

pub struct RoundRobin {
    index: usize,
    outbounds: Vec<Outbound>,
}

impl RoundRobin {
    pub fn new(outbounds: Vec<Outbound>) -> Self {
        Self {
            index: 0,
            outbounds,
        }
    }

    pub fn select(&mut self) -> &mut Outbound {
        let mut index = self.index;
        if index >= self.outbounds.len() {
            index = 0;
        }
        self.index = index + 1;
        self.outbounds.get_mut(index).unwrap()
    }
}

impl Deref for RoundRobin {
    type Target = Vec<Outbound>;

    fn deref(&self) -> &Self::Target {
        &self.outbounds
    }
}

pub struct LeastConn {
    outbounds: Vec<Outbound>,
}

impl LeastConn {
    pub fn new(outbounds: Vec<Outbound>) -> Self {
        Self { outbounds }
    }

    pub fn select(&mut self) -> &mut Outbound {
        self.outbounds
            .iter_mut()
            .min_by_key(|x| x.info().stats().active_conn())
            .unwrap()
    }
}

impl Deref for LeastConn {
    type Target = Vec<Outbound>;

    fn deref(&self) -> &Self::Target {
        &self.outbounds
    }
}
