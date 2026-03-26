use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as i64
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub lease_key: String,       // shard-1, job-export, device-group-5
    pub owner: Option<String>,   // worker ID or None if unowned
    pub counter: i64,            // incremented every renew/steal
    pub checkpoint: Option<String>, // last processed position
    pub expires_at: i64,         // epoch ms
    pub metadata: HashMap<String, String>, // anything extra
}

impl Lease {
    pub fn is_expired(&self) -> bool {
        // Add grace period to prevent clock skew issues
        epoch_ms() > self.expires_at + 2000
    }

    pub fn is_owned_by(&self, worker_id: &str) -> bool {
        self.owner.as_deref() == Some(worker_id)
    }
}
