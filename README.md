# Leasy

A generic, highly-available distributed lease manager in Rust.

**Leasy** is designed to coordinate distributed work across multiple workers. It is not tied to any specific system (like Kinesis) and can be used for:
- Shard partition assignments (Kinesis, Kafka, etc.)
- SQS queue partition reading
- Cron job deduplication (ensuring only 1 task runs a job)
- Database partition processing
- Leader election

## Features
- **Storage Agnostic:** Core logic operates over a `LeaseStorage` trait.
- **DynamoDB Backend Included:** Ship with a production-ready DynamoDB backend to prevent split-brain scenarios using conditional writes.
- **Automatic Background Rebalancing:** Workers automatically steal expired leases and balance the load without a central coordinator.
- **Clock Skew Protection:** Built-in grace periods prevent premature lease stealing.

## Usage

Add `tokio`, `aws-config` and `aws-sdk-dynamodb` along with this project to your dependencies if utilizing DynamoDB. Or simply point to `leasy` locally.

### Quickstart with DynamoDB

```rust
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use aws_config::BehaviorVersion;
use leasy::{LeaseConfig, LeaseManager};
use leasy::adapters::dynamodb::DynamoLeaseStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize DynamoDB Client
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = aws_sdk_dynamodb::Client::new(&config);

    // 2. Configure Lease Store & Manager
    let lease_duration_ms = 10_000;
    let store = Arc::new(DynamoLeaseStore::new(
        client, 
        "my-distributed-leases", // Make sure this table is created with PK `lease_key`
        lease_duration_ms
    ));
    
    let worker_id = uuid::Uuid::new_v4().to_string(); // Or use ECS Task ID
    let config = LeaseConfig::default(); // 10s lease, 3s renew, 5s rebalance
    
    let manager = Arc::new(LeaseManager::new(store, worker_id, config));

    // 3. Register your resources (e.g., Kinesis Shards, cron jobs)
    let resources = vec!["shard-0001", "shard-0002", "shard-0003"];
    for resource in resources {
        manager.ensure_lease(resource).await?;
    }

    // 4. Start background renewal & rebalance loops
    manager.clone().start_background_tasks();

    // 5. Your Processing Loop
    loop {
        let my_leases = manager.get_my_lease_keys().await?;
        
        for lease_key in my_leases {
            println!("I am processing {}!", lease_key);
            
            // Example workflow:
            // let checkpoint = manager.get_checkpoint(&lease_key).await?;
            // let new_data = fetch_data(checkpoint).await;
            // process(new_data).await;
            // manager.checkpoint(&lease_key, "new-checkpoint-seq").await?;
        }
        
        sleep(Duration::from_millis(1000)).await;
    }
}
```

## Architecture

1. **Lease Lifecycle:** Create → Acquire → Renew → Release / Expire → Steal
2. **Split Brain Prevention:** The DynamoDB backend utilizes conditional updates on a monotonically increasing counter. If two workers try to acquire the same lease, only one wins.
3. **Thundering Herd Mitigation:** Jitter is added to rebalance intervals to prevent all instances hammering the database simultaneously.
