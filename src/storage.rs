use crate::error::Result;
use crate::lease::Lease;
use async_trait::async_trait;

#[async_trait]
pub trait LeaseStorage: Send + Sync {
    async fn list_leases(&self) -> Result<Vec<Lease>>;
    async fn create_lease(&self, key: &str) -> Result<Lease>;
    
    // Conditional write — only succeeds if counter matches
    // This prevents split brain
    async fn acquire_lease(
        &self,
        lease: &Lease,
        new_owner: &str
    ) -> Result<bool>;
    
    async fn renew_lease(
        &self,
        lease: &Lease
    ) -> Result<bool>;
    
    async fn release_lease(
        &self,
        lease: &Lease
    ) -> Result<bool>;
    
    async fn update_checkpoint(
        &self,
        lease_key: &str,
        checkpoint: &str
    ) -> Result<bool>;
    
    async fn get_checkpoint(
        &self,
        lease_key: &str
    ) -> Result<Option<String>>;
}
