use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug, Clone)]
pub struct Domain {
    pub inbounds: Vec<Arc<InboundInfo>>,
    pub outbounds: Vec<Arc<OutboundInfo>>,
}

impl Default for Domain {
    fn default() -> Self {
        Self {
            inbounds: Default::default(),
            outbounds: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct InboundInfo {
    alias: String,
    write_to: Vec<String>,
    banalce_type: BalanceType,
    stats: InboundStats,
}

impl InboundInfo {
    pub fn new(alias: impl Into<String>, write_to: Vec<String>, banalce_type: BalanceType) -> Self {
        Self {
            alias: alias.into(),
            write_to,
            banalce_type,
            stats: InboundStats::new(),
        }
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }

    pub fn write_to(&self) -> &Vec<String> {
        &self.write_to
    }

    pub fn balance_type(&self) -> &BalanceType {
        &self.banalce_type
    }

    pub fn stats(&self) -> &InboundStats {
        &self.stats
    }
}

#[derive(Debug)]
pub enum BalanceType {
    RoundRobin,
    LeastConn,
}

#[derive(Debug)]
pub struct InboundStats {
    active_conn: AtomicUsize,
}

impl InboundStats {
    pub fn new() -> Self {
        Self {
            active_conn: Default::default(),
        }
    }

    pub fn active_conn(&self) -> usize {
        self.active_conn.load(Ordering::Relaxed)
    }

    pub fn active_conn_inc(&self) {
        self.active_conn.fetch_add(1, Ordering::Relaxed);
    }

    pub fn active_conn_dec(&self) {
        self.active_conn.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct OutboundInfo {
    alias: String,
    weight: isize,
    stats: OutboundStats,
}

impl OutboundInfo {
    pub fn new(alias: impl Into<String>, weight: isize) -> Self {
        Self {
            alias: alias.into(),
            weight,
            stats: OutboundStats::new(),
        }
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }

    pub fn weight(&self) -> isize {
        self.weight
    }

    pub fn stats(&self) -> &OutboundStats {
        &self.stats
    }
}

#[derive(Debug)]
pub struct OutboundStats {
    active_conn: AtomicUsize,
}

impl OutboundStats {
    pub fn new() -> Self {
        Self {
            active_conn: Default::default(),
        }
    }

    pub fn active_conn(&self) -> usize {
        self.active_conn.load(Ordering::Relaxed)
    }

    pub fn active_conn_inc(&self) {
        self.active_conn.fetch_add(1, Ordering::Relaxed);
    }

    pub fn active_conn_dec(&self) {
        self.active_conn.fetch_sub(1, Ordering::Relaxed);
    }
}
