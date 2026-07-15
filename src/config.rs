#[derive(Debug, Clone)]
pub struct LeaseConfig {
    pub lease_duration_ms: i64,
    pub renewal_interval_ms: i64,
    pub rebalance_interval_ms: i64,
    pub max_leases_per_worker: Option<usize>,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            lease_duration_ms: 10_000,
            renewal_interval_ms: 3_000,
            rebalance_interval_ms: 5_000,
            max_leases_per_worker: None,
        }
    }
}
