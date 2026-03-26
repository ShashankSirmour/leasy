use crate::config::LeaseConfig;
use crate::error::Result;
use crate::lease::Lease;
use crate::storage::LeaseStorage;
use rand::Rng;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{error, warn};

pub struct LeaseManager<S: LeaseStorage> {
    storage: Arc<S>,
    worker_id: String,
    config: LeaseConfig,
}

impl<S: LeaseStorage + 'static> LeaseManager<S> {
    pub fn new(storage: Arc<S>, worker_id: String, config: LeaseConfig) -> Self {
        Self {
            storage,
            worker_id,
            config,
        }
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Ensures a lease row exists for this key. Idempotent — safe to call
    /// from multiple workers simultaneously.
    pub async fn ensure_lease(&self, key: &str) -> Result<()> {
        let leases = self.storage.list_leases().await?;
        if !leases.iter().any(|l| l.lease_key == key) {
            match self.storage.create_lease(key).await {
                Ok(_) => {}
                // Another worker raced and created it — that's fine
                Err(crate::error::LeaseError::Conflict) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Returns lease keys currently owned by this worker.
    pub async fn get_my_lease_keys(&self) -> Result<Vec<String>> {
        let leases = self.storage.list_leases().await?;
        Ok(self
            .my_leases(&leases)
            .iter()
            .map(|l| l.lease_key.clone())
            .collect())
    }

    /// Core rebalance algorithm. Tries to reach a fair share of leases for
    /// this worker by first claiming unowned leases, then stealing expired ones.
    pub async fn rebalance(&self) -> Result<Vec<String>> {
        let all_leases = self.storage.list_leases().await?;

        let active_workers = self.count_active_workers(&all_leases);
        let total = all_leases.len();

        // ceil(total / active_workers), capped by max_leases_per_worker
        let fair_share = (total + active_workers - 1) / active_workers;
        let target = match self.config.max_leases_per_worker {
            Some(max) => fair_share.min(max),
            None => fair_share,
        };

        let my_count = self.my_leases(&all_leases).len();
        let mut remaining_deficit = target.saturating_sub(my_count);

        if remaining_deficit > 0 {
            // Priority 1: take unowned leases
            let unowned: Vec<&Lease> = all_leases
                .iter()
                .filter(|l| l.owner.is_none())
                .take(remaining_deficit)
                .collect();

            for lease in &unowned {
                if self
                    .storage
                    .acquire_lease(lease, &self.worker_id)
                    .await
                    .unwrap_or(false)
                {
                    remaining_deficit = remaining_deficit.saturating_sub(1);
                }
            }

            // Priority 2: steal expired leases (only if still short)
            if remaining_deficit > 0 {
                let expired: Vec<&Lease> = all_leases
                    .iter()
                    .filter(|l| l.is_expired() && !l.is_owned_by(&self.worker_id))
                    .take(remaining_deficit)
                    .collect();

                for lease in &expired {
                    // acquire_lease is conditional — if another worker stole it first, we get false
                    let _ = self.storage.acquire_lease(lease, &self.worker_id).await;
                }
            }
        }

        // Return final snapshot of our leases
        let updated = self.storage.list_leases().await?;
        Ok(self
            .my_leases(&updated)
            .iter()
            .map(|l| l.lease_key.clone())
            .collect())
    }

    /// Renew all leases owned by this worker. Uses concurrent requests to
    /// minimize total latency when holding many leases.
    pub async fn renew_my_leases(&self) -> Result<()> {
        let leases = self.storage.list_leases().await?;
        let my_leases = self.my_leases(&leases);

        let mut handles = tokio::task::JoinSet::new();

        for lease in my_leases {
            let storage = self.storage.clone();
            let l = lease.clone();
            handles.spawn(async move {
                if let Err(e) = storage.renew_lease(&l).await {
                    warn!("Failed to renew lease {}: {}", l.lease_key, e);
                }
            });
        }

        while let Some(res) = handles.join_next().await {
            if let Err(e) = res {
                error!("Task join error during renewal: {}", e);
            }
        }

        Ok(())
    }

    pub async fn get_checkpoint(&self, lease_key: &str) -> Result<Option<String>> {
        self.storage.get_checkpoint(lease_key).await
    }

    pub async fn checkpoint(&self, lease_key: &str, checkpoint: &str) -> Result<()> {
        self.storage.update_checkpoint(lease_key, checkpoint).await?;
        Ok(())
    }

    /// Count distinct non-expired owners. Returns at least 1 (this worker).
    fn count_active_workers(&self, leases: &[Lease]) -> usize {
        let mut owners = std::collections::HashSet::new();
        // Always count ourselves even if we hold zero leases yet
        owners.insert(&self.worker_id);
        for lease in leases {
            if !lease.is_expired() {
                if let Some(ref owner) = lease.owner {
                    owners.insert(owner);
                }
            }
        }
        owners.len().max(1)
    }

    fn my_leases<'a>(&self, leases: &'a [Lease]) -> Vec<&'a Lease> {
        leases
            .iter()
            .filter(|l| l.is_owned_by(&self.worker_id))
            .collect()
    }

    /// Spawns background tokio tasks for periodic renewal and rebalancing.
    /// Call this once after creating the manager.
    pub fn start_background_tasks(self: Arc<Self>) {
        let manager_renew = self.clone();

        // Renewal loop
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(
                    manager_renew.config.renewal_interval_ms as u64,
                ))
                .await;

                if let Err(e) = manager_renew.renew_my_leases().await {
                    error!("Renewal failed: {}", e);
                }
            }
        });

        // Rebalance loop with jitter to prevent thundering herd
        let manager_rebalance = self.clone();
        tokio::spawn(async move {
            let mut rng = rand::thread_rng();
            loop {
                let jitter = rng.gen_range(0..1000u64);
                sleep(Duration::from_millis(
                    (manager_rebalance.config.rebalance_interval_ms as u64) + jitter,
                ))
                .await;

                match manager_rebalance.rebalance().await {
                    Ok(leases) => tracing::debug!("Holding {} leases: {:?}", leases.len(), leases),
                    Err(e) => error!("Rebalance failed: {}", e),
                }
            }
        });
    }
}
