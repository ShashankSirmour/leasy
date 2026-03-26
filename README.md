# Leasy

A generic distributed lease manager in Rust — storage agnostic, split-brain safe.

## Use Cases
- **Kinesis shard assignment** — replace KCL with your own lease coordination
- **Cron job deduplication** — only 1 worker runs a scheduled job
- **Leader election** — elect a single master node
- **Database partition processing** — coordinate bulk data jobs

## Project Structure

```
src/
├── lib.rs          # Public exports
├── config.rs       # LeaseConfig with tunable intervals
├── error.rs        # LeaseError enum
├── lease.rs        # Lease struct + helpers
├── manager.rs      # LeaseManager — core rebalance & renewal engine
├── storage.rs      # LeaseStorage trait (implement for any backend)
└── adapters/
    ├── mod.rs      # Feature-gated adapter modules
    └── dynamodb.rs # DynamoDB backend (conditional writes)
```

## Quick Start

```toml
# Cargo.toml of your consuming project
[dependencies]
leasy = { path = "../lib/Leasy", features = ["dynamodb"] }
tokio = { version = "1", features = ["full"] }
aws-sdk-dynamodb = "1"
aws-config = "1"
uuid = { version = "1", features = ["v4"] }
tracing-subscriber = "0.3"
```

### DynamoDB Table Setup

Create a table with partition key `lease_key` (String). No sort key needed.

```bash
aws dynamodb create-table \
  --table-name my-leases \
  --attribute-definitions AttributeName=lease_key,AttributeType=S \
  --key-schema AttributeName=lease_key,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST
```

### Basic Usage

```rust
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use leasy::{LeaseConfig, LeaseManager, DynamoLeaseStore};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let dynamo = aws_sdk_dynamodb::Client::new(&aws_config);

    let store = Arc::new(DynamoLeaseStore::new(dynamo, "my-leases", 10_000));
    let worker_id = uuid::Uuid::new_v4().to_string();
    let manager = Arc::new(LeaseManager::new(store, worker_id, LeaseConfig::default()));

    // Register resources
    for key in &["job-a", "job-b", "job-c"] {
        manager.ensure_lease(key).await?;
    }

    manager.clone().start_background_tasks();

    loop {
        for key in manager.get_my_lease_keys().await? {
            println!("Processing {}", key);
        }
        sleep(Duration::from_secs(1)).await;
    }
}
```

---

### Kinesis Shard Consumer (replacing KCL)

This is the primary use case — each ECS Fargate task runs this, and Leasy
automatically distributes shards across all running tasks.

```rust
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use aws_config::BehaviorVersion;
use leasy::{LeaseConfig, LeaseManager, DynamoLeaseStore};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let aws_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let dynamo = aws_sdk_dynamodb::Client::new(&aws_config);
    let kinesis = aws_sdk_kinesis::Client::new(&aws_config);

    // 1. Create lease store + manager
    let lease_duration_ms = 10_000; // 10s — gives 7s buffer with 3s renewal
    let store = Arc::new(DynamoLeaseStore::new(
        dynamo,
        "kinesis-shard-leases",
        lease_duration_ms,
    ));

    // On ECS Fargate, use the task ARN for stable worker identity.
    // Falls back to UUID if not on ECS.
    let worker_id = std::env::var("ECS_TASK_ARN")
        .unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());

    let config = LeaseConfig {
        lease_duration_ms: 10_000,
        renewal_interval_ms: 3_000,
        rebalance_interval_ms: 5_000,
        max_leases_per_worker: None,
    };

    let manager = Arc::new(LeaseManager::new(store, worker_id, config));

    // 2. Discover shards and register them as leases
    let stream_name = "my-kinesis-stream";
    let shards = kinesis
        .list_shards()
        .stream_name(stream_name)
        .send()
        .await?
        .shards
        .unwrap_or_default();

    for shard in &shards {
        manager.ensure_lease(&shard.shard_id).await?;
    }

    // 3. Start background renewal + rebalance loops
    manager.clone().start_background_tasks();

    // 4. Processing loop — only processes shards assigned to this worker
    loop {
        let my_shards = manager.get_my_lease_keys().await?;
        tracing::info!("Processing {} shards", my_shards.len());

        for shard_id in &my_shards {
            // Get last checkpoint (sequence number) for this shard
            let checkpoint = manager.get_checkpoint(shard_id).await?;

            let mut iter_req = kinesis.get_shard_iterator()
                .stream_name(stream_name)
                .shard_id(shard_id);

            iter_req = match &checkpoint {
                Some(seq) => iter_req
                    .shard_iterator_type(aws_sdk_kinesis::types::ShardIteratorType::AfterSequenceNumber)
                    .starting_sequence_number(seq),
                None => iter_req
                    .shard_iterator_type(aws_sdk_kinesis::types::ShardIteratorType::TrimHorizon),
            };

            let iterator = iter_req.send().await?.shard_iterator;

            if let Some(iter) = iterator {
                let records_output = kinesis
                    .get_records()
                    .shard_iterator(&iter)
                    .limit(1000)
                    .send()
                    .await?;

                let records = records_output.records;

                for record in &records {
                    // --- Your business logic here ---
                    let data = record.data().as_ref();
                    tracing::debug!(
                        "Shard {} seq {}: {} bytes",
                        shard_id,
                        record.sequence_number(),
                        data.len()
                    );
                }

                // Checkpoint the last sequence number
                if let Some(last) = records.last() {
                    manager
                        .checkpoint(shard_id, last.sequence_number())
                        .await?;
                }
            }
        }

        sleep(Duration::from_millis(200)).await;
    }
}
```

## Architecture

### Lease Lifecycle
```
Create → Acquire → Renew → Release
                     ↓
                  Expire → Steal (by another worker)
```

### Split Brain Prevention
DynamoDB conditional writes on a monotonic `counter` field. If two workers
race to acquire/renew the same lease, exactly one succeeds — the other gets
`false` and backs off.

### Rebalance Algorithm
```
total_leases  = 10
active_workers = 3
target_per_worker = ceil(10/3) = 4

Worker A → 4 leases
Worker B → 3 leases
Worker C → 3 leases
```
Priority: unowned leases first, then expired leases. Jitter on the rebalance
interval prevents thundering herd on DynamoDB.

### Failure Recovery
Worker crashes → stops renewing → lease `expires_at` passes →
next rebalance cycle on surviving workers detects expiry →
they steal the lease → processing resumes automatically.
