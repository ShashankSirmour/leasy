#[derive(Debug, Clone)]
pub struct LeaseConfig {
    pub lease_duration_ms: i64,    // how long lease is valid
    pub renewal_interval_ms: i64,  // how often to renew
    pub rebalance_interval_ms: i64,// how often to rebalance
    pub max_leases_per_worker: Option<usize>, // cap if needed
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            lease_duration_ms: 10_000,    // 10 sec
            renewal_interval_ms: 3_000,   // 3 sec
            rebalance_interval_ms: 5_000, // 5 sec
            max_leases_per_worker: None,
        }
    }
}
