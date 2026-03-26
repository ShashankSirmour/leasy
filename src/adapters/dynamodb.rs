use crate::error::{LeaseError, Result};
use crate::lease::{epoch_ms, Lease};
use crate::storage::LeaseStorage;
use async_trait::async_trait;
use aws_sdk_dynamodb::{
    types::AttributeValue,
    Client,
};
use std::collections::HashMap;

pub struct DynamoLeaseStore {
    client: Client,
    table_name: String,
    lease_duration_ms: i64,
}

impl DynamoLeaseStore {
    pub fn new(client: Client, table_name: impl Into<String>, lease_duration_ms: i64) -> Self {
        Self {
            client,
            table_name: table_name.into(),
            lease_duration_ms,
        }
    }

    fn parse_lease(item: &HashMap<String, AttributeValue>) -> Lease {
        let lease_key = item.get("lease_key")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();
            
        let owner = item.get("owner")
            .and_then(|v| v.as_s().ok())
            .cloned();
            
        let counter = item.get("counter")
            .and_then(|v| v.as_n().ok())
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);
            
        let expires_at = item.get("expires_at")
            .and_then(|v| v.as_n().ok())
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);
            
        let checkpoint = item.get("checkpoint")
            .and_then(|v| v.as_s().ok())
            .cloned();

        Lease {
            lease_key,
            owner,
            counter,
            checkpoint,
            expires_at,
            metadata: HashMap::new(),
        }
    }
}

#[async_trait]
impl LeaseStorage for DynamoLeaseStore {
    async fn list_leases(&self) -> Result<Vec<Lease>> {
        let mut leases = Vec::new();
        let mut last_evaluated_key = None;

        loop {
            let mut req = self.client.scan().table_name(&self.table_name);
            if let Some(key) = last_evaluated_key {
                req = req.set_exclusive_start_key(Some(key));
            }

            let res = req.send().await.map_err(|e| LeaseError::Storage(e.to_string()))?;

            if let Some(items) = res.items {
                for item in items {
                    leases.push(Self::parse_lease(&item));
                }
            }

            last_evaluated_key = res.last_evaluated_key;
            if last_evaluated_key.is_none() {
                break;
            }
        }

        Ok(leases)
    }

    async fn create_lease(&self, key: &str) -> Result<Lease> {
        let lease = Lease {
            lease_key: key.to_string(),
            owner: None,
            counter: 0,
            checkpoint: None,
            expires_at: 0,
            metadata: HashMap::new(),
        };

        self.client
            .put_item()
            .table_name(&self.table_name)
            .item("lease_key", AttributeValue::S(key.to_string()))
            .item("counter", AttributeValue::N("0".to_string()))
            .item("expires_at", AttributeValue::N("0".to_string()))
            .condition_expression("attribute_not_exists(lease_key)")
            .send()
            .await
            .map_err(|e| {
                if e.to_string().contains("ConditionalCheckFailedException") {
                    LeaseError::Conflict
                } else {
                    LeaseError::Storage(e.to_string())
                }
            })?;

        Ok(lease)
    }

    async fn acquire_lease(&self, lease: &Lease, new_owner: &str) -> Result<bool> {
        let new_counter = lease.counter + 1;
        let new_expires = epoch_ms() + self.lease_duration_ms;

        let result = self.client
            .update_item()
            .table_name(&self.table_name)
            .key("lease_key", AttributeValue::S(lease.lease_key.clone()))
            .update_expression("SET #owner = :owner, #counter = :new_counter, expires_at = :expires")
            .condition_expression("#counter = :old_counter")
            .expression_attribute_names("#owner", "owner")
            .expression_attribute_names("#counter", "counter")
            .expression_attribute_values(":old_counter", AttributeValue::N(lease.counter.to_string()))
            .expression_attribute_values(":new_counter", AttributeValue::N(new_counter.to_string()))
            .expression_attribute_values(":owner", AttributeValue::S(new_owner.to_string()))
            .expression_attribute_values(":expires", AttributeValue::N(new_expires.to_string()))
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(e) if e.to_string().contains("ConditionalCheckFailedException") => Ok(false),
            Err(e) => Err(LeaseError::Storage(e.to_string())),
        }
    }

    async fn renew_lease(&self, lease: &Lease) -> Result<bool> {
        let new_counter = lease.counter + 1;
        let new_expires = epoch_ms() + self.lease_duration_ms;
        let current_owner = lease.owner.as_deref().unwrap_or("");
        
        // When renewing, optionally ensure it's still ours with the matching condition
        let result = self.client
            .update_item()
            .table_name(&self.table_name)
            .key("lease_key", AttributeValue::S(lease.lease_key.clone()))
            .update_expression("SET #counter = :new_counter, expires_at = :expires")
            .condition_expression("#counter = :old_counter AND #owner = :current_owner")
            .expression_attribute_names("#owner", "owner")
            .expression_attribute_names("#counter", "counter")
            .expression_attribute_values(":old_counter", AttributeValue::N(lease.counter.to_string()))
            .expression_attribute_values(":new_counter", AttributeValue::N(new_counter.to_string()))
            .expression_attribute_values(":expires", AttributeValue::N(new_expires.to_string()))
            .expression_attribute_values(":current_owner", AttributeValue::S(current_owner.to_string()))
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(e) if e.to_string().contains("ConditionalCheckFailedException") => Ok(false),
            Err(e) => Err(LeaseError::Storage(e.to_string())),
        }
    }

    async fn release_lease(&self, lease: &Lease) -> Result<bool> {
        let result = self.client
            .update_item()
            .table_name(&self.table_name)
            .key("lease_key", AttributeValue::S(lease.lease_key.clone()))
            .update_expression("REMOVE #owner SET #counter = :new_counter, expires_at = :zero")
            .condition_expression("#counter = :old_counter AND #owner = :current_owner")
            .expression_attribute_names("#owner", "owner")
            .expression_attribute_names("#counter", "counter")
            .expression_attribute_values(":old_counter", AttributeValue::N(lease.counter.to_string()))
            .expression_attribute_values(":new_counter", AttributeValue::N((lease.counter + 1).to_string()))
            .expression_attribute_values(":zero", AttributeValue::N("0".to_string()))
            .expression_attribute_values(":current_owner", AttributeValue::S(lease.owner.as_deref().unwrap_or("").to_string()))
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(e) if e.to_string().contains("ConditionalCheckFailedException") => Ok(false),
            Err(e) => Err(LeaseError::Storage(e.to_string())),
        }
    }

    async fn update_checkpoint(&self, lease_key: &str, checkpoint: &str) -> Result<bool> {
        self.client
            .update_item()
            .table_name(&self.table_name)
            .key("lease_key", AttributeValue::S(lease_key.to_string()))
            .update_expression("SET checkpoint = :checkpoint")
            .expression_attribute_values(":checkpoint", AttributeValue::S(checkpoint.to_string()))
            .send()
            .await
            .map_err(|e| LeaseError::Storage(e.to_string()))?;

        Ok(true)
    }

    async fn get_checkpoint(&self, lease_key: &str) -> Result<Option<String>> {
        let result = self.client
            .get_item()
            .table_name(&self.table_name)
            .key("lease_key", AttributeValue::S(lease_key.to_string()))
            .projection_expression("checkpoint")
            .send()
            .await
            .map_err(|e| LeaseError::Storage(e.to_string()))?;

        if let Some(item) = result.item {
            if let Some(AttributeValue::S(checkpoint)) = item.get("checkpoint") {
                return Ok(Some(checkpoint.clone()));
            }
        }

        Ok(None)
    }
}
