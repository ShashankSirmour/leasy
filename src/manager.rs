use crate::lease::Lease;
use crate::storage::LeaseStorage;
use crate::config::LeaseConfig;
use crate::error::Result;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{info, error, warn};

pub struct LeaseManager<S: LeaseStorage> {
    storage: Arc<S>,
    worker_id: String,
    config: LeaseConfig,
}

impl<S: LeaseStorage + 'static> LeaseManager<S> {
    pub fn new(storage: Arc<S>, worker_id: String, config: LeaseConfig) -> Self {
        Self { storage, worker_id, config }
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    pub async fn ensure_lease(&self, key: &str) -> Result<()> {
        let leases = self.storage.list_leases().await?;
        if !leases.iter().any(|l| l.lease_key == key) {
            self.storage.create_lease(key).await?;
        }
        Ok(())
    }

    pub async fn get_my_lease_keys(&self) -> Result<Vec<String>> {
        let leases = self.storage.list_leases().await?;
        Ok(self.my_leases(&leases).iter().map(|l| l.lease_key.clone()).collect())
    }

    pub async fn rebalance(&self) -> Result<Vec<String>> {
        let all_leases = self.storage.list_leases().await?;
        
        let active_workers = self.count_active_workers(&all_leases);
        let total = all_leases.len();
        
        let target = match self.config.max_leases_per_worker {
            Some(max) => max.min((total + active_workers - 1).checked_div(active_workers).unwrap_or(total)),
            None => if active_workers == 0 { total } else { (total + active_workers - 1) / active_workers },
        };

        let my_leases = self.my_leases(&all_leases);
        let deficit = target.saturating_sub(my_leases.len());

        if deficit > 0 {
            // Priority 1: take unowned leases
            let unowned: Vec<&Lease> = all_leases.iter()
                .filter(|l| l.owner.is_none())
                .take(deficit)
                .collect();

            for lease in unowned {
                self.storage.acquire_lease(lease, &self.worker_id).await?;
            }

            // Priority 2: steal expired leases
            let expired: Vec<&Lease> = all_leases.iter()
                .filter(|l| l.is_expired() && !l.is_owned_by(&self.worker_id))
                .take(deficit)
                .collect();

            for lease in expired {
                self.storage.acquire_lease(lease, &self.worker_id).await?;
            }
        }

        let updated = self.storage.list_leases().await?;
        Ok(self.my_leases(&updated)
            .iter()
            .map(|l| l.lease_key.clone())
            .collect())
    }

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

    fn count_active_workers(&self, leases: &[Lease]) -> usize {
        leases.iter()
            .filter_map(|l| if !l.is_expired() { l.owner.as_ref() } else { None })
            .collect::<std::collections::HashSet<_>>()
            .len()
            .max(1)
    }

    fn my_leases<'a>(&self, leases: &'a [Lease]) -> Vec<&'a Lease> {
        leases.iter()
            .filter(|l| l.is_owned_by(&self.worker_id))
            .collect()
    }

    pub fn start_background_tasks(self: Arc<Self>) {
        let manager_renew = self.clone();
        
        // Renewal loop
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(
                    manager_renew.config.renewal_interval_ms as u64
                )).await;
                
                if let Err(e) = manager_renew.renew_my_leases().await {
                    error!("Renewal failed: {}", e);
                }
            }
        });

        // Rebalance loop
        let manager_rebalance = self.clone();
        tokio::spawn(async move {
            loop {
                let jitter = rand::random::<u64>() % 1000;
                sleep(Duration::from_millis(
                    (manager_rebalance.config.rebalance_interval_ms as u64) + jitter
                )).await;

                match manager_rebalance.rebalance().await {
                    Ok(leases) => tracing::debug!("Holding leases: {:?}", leases),
                    Err(e) => error!("Rebalance failed: {}", e),
                }
            }
        });
    }
}
