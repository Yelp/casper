use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use once_cell::sync::Lazy;

#[derive(Debug, Default)]
pub struct Stats {
    pub total_conns: AtomicUsize,
    pub total_requests: AtomicUsize,
    pub active_conns: ActiveCounter,
    pub active_requests: ActiveCounter,
}

pub static GLOBAL_STATS: Lazy<Stats> = Lazy::new(|| Stats::new());

impl Stats {
    pub fn new() -> Self {
        Stats::default()
    }

    // pub fn total_conns(&self) -> usize {
    //     self.total_conns.load(Ordering::Relaxed)
    // }

    pub fn inc_total_conns(&self, n: usize) {
        self.total_conns.fetch_add(n, Ordering::Relaxed);
    }

    // pub fn total_requests(&self) -> usize {
    //     self.total_requests.load(Ordering::Relaxed)
    // }

    pub fn inc_total_requests(&self, n: usize) {
        self.total_requests.fetch_add(n, Ordering::Relaxed);
    }

    pub fn active_conns(&self) -> usize {
        self.active_conns.get()
    }

    pub fn inc_active_conns(&self, n: usize) -> ActiveCounterHandler {
        self.active_conns.inc(n)
    }

    pub fn active_requests(&self) -> usize {
        self.active_requests.get()
    }

    pub fn inc_active_requests(&self, n: usize) -> ActiveCounterHandler {
        self.active_requests.inc(n)
    }
}

#[derive(Debug, Default, Clone)]
pub struct ActiveCounter(Arc<AtomicUsize>);

#[derive(Debug)]
pub struct ActiveCounterHandler(Arc<AtomicUsize>, usize);

impl ActiveCounter {
    pub fn new(v: usize) -> Self {
        ActiveCounter(Arc::new(AtomicUsize::new(v)))
    }

    pub fn get(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }

    pub fn inc(&self, n: usize) -> ActiveCounterHandler {
        self.0.fetch_add(n, Ordering::Relaxed);
        ActiveCounterHandler(Arc::clone(&self.0), n)
    }
}

impl Drop for ActiveCounterHandler {
    fn drop(&mut self) {
        self.0.fetch_sub(self.1, Ordering::Relaxed);
    }
}
