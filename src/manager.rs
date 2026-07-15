use crate::config::LeaseConfig;
use crate::error::{LeaseError, Result};
use crate::lease::{epoch_ms, Lease};
use crate::storage::LeaseStorage;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{debug, warn};

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
        if self.storage.list_leases().await?.iter().any(|l| l.lease_key == key) {
            return Ok(());
        }
        match self.storage.create_lease(key).await {
            // Conflict = another worker raced and created it — that's fine
            Ok(_) | Err(LeaseError::Conflict) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Returns lease keys currently owned by this worker.
    pub async fn get_my_lease_keys(&self) -> Result<Vec<String>> {
        Ok(self.my_lease_keys(&self.storage.list_leases().await?))
    }

    /// Core rebalance algorithm. Tries to reach a fair share of leases for
    /// this worker by first claiming unowned leases, then stealing expired ones.
    pub async fn rebalance(&self) -> Result<Vec<String>> {
        let leases = self.storage.list_leases().await?;

        let fair_share = leases.len().div_ceil(self.active_workers(&leases));
        let target = self
            .config
            .max_leases_per_worker
            .map_or(fair_share, |max| fair_share.min(max));

        let owned = self.my_leases(&leases).count();
        let mut deficit = target.saturating_sub(owned);

        // Priority 1: take unowned leases
        for lease in leases.iter().filter(|l| l.owner.is_none()).take(deficit) {
            if self
                .storage
                .acquire_lease(lease, &self.worker_id)
                .await
                .unwrap_or(false)
            {
                deficit -= 1;
            }
        }

        // Priority 2: steal expired leases. acquire_lease is conditional —
        // if another worker stole it first, it returns false.
        for lease in leases
            .iter()
            .filter(|l| l.is_expired() && !l.is_owned_by(&self.worker_id))
            .take(deficit)
        {
            let _ = self.storage.acquire_lease(lease, &self.worker_id).await;
        }

        // Return final snapshot of our leases
        Ok(self.my_lease_keys(&self.storage.list_leases().await?))
    }

    /// Renew all leases owned by this worker, concurrently.
    pub async fn renew_my_leases(&self) -> Result<()> {
        let leases = self.storage.list_leases().await?;
        let mut tasks = tokio::task::JoinSet::new();

        for lease in self.my_leases(&leases) {
            let storage = self.storage.clone();
            let lease = lease.clone();
            tasks.spawn(async move {
                if let Err(e) = storage.renew_lease(&lease).await {
                    warn!("Failed to renew lease {}: {}", lease.lease_key, e);
                }
            });
        }
        while tasks.join_next().await.is_some() {}

        Ok(())
    }

    pub async fn get_checkpoint(&self, lease_key: &str) -> Result<Option<String>> {
        self.storage.get_checkpoint(lease_key).await
    }

    pub async fn checkpoint(&self, lease_key: &str, checkpoint: &str) -> Result<()> {
        self.storage.update_checkpoint(lease_key, checkpoint).await.map(|_| ())
    }

    /// Count distinct non-expired owners, always including this worker.
    fn active_workers(&self, leases: &[Lease]) -> usize {
        leases
            .iter()
            .filter(|l| !l.is_expired())
            .filter_map(|l| l.owner.as_deref())
            .chain([self.worker_id.as_str()])
            .collect::<HashSet<_>>()
            .len()
    }

    fn my_leases<'a>(&'a self, leases: &'a [Lease]) -> impl Iterator<Item = &'a Lease> {
        leases.iter().filter(|l| l.is_owned_by(&self.worker_id))
    }

    fn my_lease_keys(&self, leases: &[Lease]) -> Vec<String> {
        self.my_leases(leases).map(|l| l.lease_key.clone()).collect()
    }

    /// Spawns background tokio tasks for periodic renewal and rebalancing.
    /// Call this once after creating the manager.
    pub fn start_background_tasks(self: Arc<Self>) {
        let renewer = self.clone();
        tokio::spawn(async move {
            let interval = renewer.config.renewal_interval_ms as u64;
            loop {
                sleep(Duration::from_millis(interval)).await;
                if let Err(e) = renewer.renew_my_leases().await {
                    warn!("Renewal failed: {}", e);
                }
            }
        });

        tokio::spawn(async move {
            let interval = self.config.rebalance_interval_ms as u64;
            loop {
                // Jitter prevents thundering herd on the storage backend
                let jitter = (epoch_ms() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) % 1000;
                sleep(Duration::from_millis(interval + jitter)).await;

                match self.rebalance().await {
                    Ok(keys) => debug!("Holding {} leases: {:?}", keys.len(), keys),
                    Err(e) => warn!("Rebalance failed: {}", e),
                }
            }
        });
    }
}
