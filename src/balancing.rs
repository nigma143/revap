use std::ops::Deref;

use crate::bound::Gateway;

pub enum LoadBalancing {
    RoundRobin(RoundRobin),
    LeastConn(LeastConn),
}

impl LoadBalancing {
    pub fn round_robin(gateways: Vec<Gateway>) -> Self {
        LoadBalancing::RoundRobin(RoundRobin::new(gateways))
    }

    pub fn least_conn(gateways: Vec<Gateway>) -> Self {
        LoadBalancing::LeastConn(LeastConn::new(gateways))
    }

    pub fn select(&mut self) -> &mut Gateway {
        match self {
            LoadBalancing::RoundRobin(o) => o.select(),
            LoadBalancing::LeastConn(o) => o.select(),
        }
    }
}

impl Deref for LoadBalancing {
    type Target = Vec<Gateway>;

    fn deref(&self) -> &Self::Target {
        match self {
            LoadBalancing::RoundRobin(o) => o,
            LoadBalancing::LeastConn(o) => o,
        }
    }
}

pub struct RoundRobin {
    index: usize,
    gateways: Vec<Gateway>,
}

impl RoundRobin {
    pub fn new(gateways: Vec<Gateway>) -> Self {
        Self { index: 0, gateways }
    }

    pub fn select(&mut self) -> &mut Gateway {
        let mut index = self.index;
        if index >= self.gateways.len() {
            index = 0;
        }
        self.index = index + 1;
        self.gateways.get_mut(index).unwrap()
    }
}

impl Deref for RoundRobin {
    type Target = Vec<Gateway>;

    fn deref(&self) -> &Self::Target {
        &self.gateways
    }
}

pub struct LeastConn {
    gateways: Vec<Gateway>,
}

impl LeastConn {
    pub fn new(gateways: Vec<Gateway>) -> Self {
        Self { gateways }
    }

    pub fn select(&mut self) -> &mut Gateway {
        self.gateways
            .iter_mut()
            .min_by_key(|x| x.active_conn())
            .unwrap()
    }
}

impl Deref for LeastConn {
    type Target = Vec<Gateway>;

    fn deref(&self) -> &Self::Target {
        &self.gateways
    }
}
