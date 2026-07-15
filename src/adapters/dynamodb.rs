use crate::error::{LeaseError, Result};
use crate::lease::{epoch_ms, Lease};
use crate::storage::LeaseStorage;
use async_trait::async_trait;
use aws_sdk_dynamodb::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::{types::AttributeValue, Client};
use std::collections::HashMap;

type Item = HashMap<String, AttributeValue>;

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn n(v: i64) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

fn get_s(item: &Item, key: &str) -> Option<String> {
    item.get(key).and_then(|v| v.as_s().ok()).cloned()
}

fn get_n(item: &Item, key: &str) -> i64 {
    item.get(key)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

fn storage_err(e: impl std::error::Error) -> LeaseError {
    LeaseError::Storage(DisplayErrorContext(e).to_string())
}

fn is_condition_failed<E: ProvideErrorMetadata>(err: &SdkError<E>) -> bool {
    err.code() == Some("ConditionalCheckFailedException")
}

/// Maps a conditional write result: success → true, condition failed → false.
fn cond_result<T, E>(res: std::result::Result<T, SdkError<E>>) -> Result<bool>
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    match res {
        Ok(_) => Ok(true),
        Err(ref e) if is_condition_failed(e) => Ok(false),
        Err(e) => Err(storage_err(e)),
    }
}

pub struct DynamoLeaseStore {
    client: Client,
    table_name: String,
    lease_duration_ms: i64,
}

impl DynamoLeaseStore {
    pub async fn new(
        client: Client,
        table_name: impl Into<String>,
        lease_duration_ms: i64,
    ) -> Result<Self> {
        Ok(Self {
            client,
            table_name: table_name.into(),
            lease_duration_ms,
        })
    }

    fn parse_lease(item: &Item) -> Lease {
        Lease {
            lease_key: get_s(item, "lease_key").unwrap_or_default(),
            owner: get_s(item, "owner"),
            counter: get_n(item, "counter"),
            checkpoint: get_s(item, "checkpoint"),
            expires_at: get_n(item, "expires_at"),
            metadata: HashMap::new(),
        }
    }
}

#[async_trait]
impl LeaseStorage for DynamoLeaseStore {
    async fn list_leases(&self) -> Result<Vec<Lease>> {
        let mut stream = self
            .client
            .scan()
            .table_name(&self.table_name)
            .into_paginator()
            .items()
            .send();

        let mut leases = Vec::new();
        while let Some(item) = stream.next().await {
            leases.push(Self::parse_lease(&item.map_err(storage_err)?));
        }
        Ok(leases)
    }

    async fn create_lease(&self, key: &str) -> Result<Lease> {
        self.client
            .put_item()
            .table_name(&self.table_name)
            .item("lease_key", s(key))
            .item("counter", n(0))
            .item("expires_at", n(0))
            .condition_expression("attribute_not_exists(lease_key)")
            .send()
            .await
            .map_err(|e| {
                if is_condition_failed(&e) {
                    LeaseError::Conflict
                } else {
                    storage_err(e)
                }
            })?;

        Ok(Lease {
            lease_key: key.to_string(),
            owner: None,
            counter: 0,
            checkpoint: None,
            expires_at: 0,
            metadata: HashMap::new(),
        })
    }

    /// Atomic acquire: conditional on counter matching the value we read.
    /// If another worker changed the counter since our read, this fails safely.
    async fn acquire_lease(&self, lease: &Lease, new_owner: &str) -> Result<bool> {
        let result = self
            .client
            .update_item()
            .table_name(&self.table_name)
            .key("lease_key", s(&lease.lease_key))
            .update_expression("SET #owner = :owner, #counter = :new_counter, #expires = :expires")
            .condition_expression("#counter = :old_counter")
            .expression_attribute_names("#owner", "owner")
            .expression_attribute_names("#counter", "counter")
            .expression_attribute_names("#expires", "expires_at")
            .expression_attribute_values(":old_counter", n(lease.counter))
            .expression_attribute_values(":new_counter", n(lease.counter + 1))
            .expression_attribute_values(":owner", s(new_owner))
            .expression_attribute_values(":expires", n(epoch_ms() + self.lease_duration_ms))
            .send()
            .await;

        cond_result(result)
    }

    /// Renew: extends expiry, but only if we still own it (counter + owner match).
    async fn renew_lease(&self, lease: &Lease) -> Result<bool> {
        let result = self
            .client
            .update_item()
            .table_name(&self.table_name)
            .key("lease_key", s(&lease.lease_key))
            .update_expression("SET #counter = :new_counter, #expires = :expires")
            .condition_expression("#counter = :old_counter AND #owner = :current_owner")
            .expression_attribute_names("#owner", "owner")
            .expression_attribute_names("#counter", "counter")
            .expression_attribute_names("#expires", "expires_at")
            .expression_attribute_values(":old_counter", n(lease.counter))
            .expression_attribute_values(":new_counter", n(lease.counter + 1))
            .expression_attribute_values(":expires", n(epoch_ms() + self.lease_duration_ms))
            .expression_attribute_values(":current_owner", s(lease.owner.as_deref().unwrap_or("")))
            .send()
            .await;

        cond_result(result)
    }

    /// Release: removes owner, resets expiry — only if we still own it.
    async fn release_lease(&self, lease: &Lease) -> Result<bool> {
        let result = self
            .client
            .update_item()
            .table_name(&self.table_name)
            .key("lease_key", s(&lease.lease_key))
            .update_expression("SET #counter = :new_counter, #expires = :zero REMOVE #owner")
            .condition_expression("#counter = :old_counter AND #owner = :current_owner")
            .expression_attribute_names("#owner", "owner")
            .expression_attribute_names("#counter", "counter")
            .expression_attribute_names("#expires", "expires_at")
            .expression_attribute_values(":old_counter", n(lease.counter))
            .expression_attribute_values(":new_counter", n(lease.counter + 1))
            .expression_attribute_values(":zero", n(0))
            .expression_attribute_values(":current_owner", s(lease.owner.as_deref().unwrap_or("")))
            .send()
            .await;

        cond_result(result)
    }

    /// Unconditional SET — the caller (LeaseManager) only checkpoints leases
    /// it owns. Pass worker_id through and condition on #owner to harden.
    async fn update_checkpoint(&self, lease_key: &str, checkpoint: &str) -> Result<bool> {
        self.client
            .update_item()
            .table_name(&self.table_name)
            .key("lease_key", s(lease_key))
            .update_expression("SET #checkpoint = :checkpoint")
            .expression_attribute_names("#checkpoint", "checkpoint")
            .expression_attribute_values(":checkpoint", s(checkpoint))
            .send()
            .await
            .map_err(storage_err)?;

        Ok(true)
    }

    async fn get_checkpoint(&self, lease_key: &str) -> Result<Option<String>> {
        let result = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key("lease_key", s(lease_key))
            .projection_expression("#checkpoint")
            .expression_attribute_names("#checkpoint", "checkpoint")
            .send()
            .await
            .map_err(storage_err)?;

        Ok(result.item.as_ref().and_then(|i| get_s(i, "checkpoint")))
    }
}
