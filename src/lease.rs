use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Grace period on expiry checks to absorb clock skew between workers.
const CLOCK_SKEW_GRACE_MS: i64 = 2_000;

pub fn epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as i64
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub lease_key: String,
    pub owner: Option<String>,
    /// Incremented on every acquire/renew/release — the conditional-write guard.
    pub counter: i64,
    pub checkpoint: Option<String>,
    /// Epoch ms.
    pub expires_at: i64,
    pub metadata: HashMap<String, String>,
}

impl Lease {
    pub fn is_expired(&self) -> bool {
        epoch_ms() > self.expires_at + CLOCK_SKEW_GRACE_MS
    }

    pub fn is_owned_by(&self, worker_id: &str) -> bool {
        self.owner.as_deref() == Some(worker_id)
    }
}
