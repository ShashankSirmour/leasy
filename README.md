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

## Dependencies

Add these to your consuming project's `Cargo.toml`:

```toml
[dependencies]
leasy = { path = "../lib/Leasy", features = ["dynamodb"] }
tokio = { version = "1", features = ["full"] }
aws-sdk-dynamodb = "1"
aws-sdk-kinesis = "1"
aws-config = "1"
uuid = { version = "1", features = ["v4"] }
anyhow = "1"
tracing = "0.1"
tracing-subscriber = "0.3"
```

---

## DynamoDB Table Setup

Create a table with partition key `lease_key` (String). No sort key needed.

```bash
aws dynamodb create-table \
  --table-name kinesis-shard-leases \
  --attribute-definitions AttributeName=lease_key,AttributeType=S \
  --key-schema AttributeName=lease_key,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST
```

---

## Basic Usage

```rust
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use leasy::{LeaseConfig, LeaseManager, DynamoLeaseStore};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let dynamo = aws_sdk_dynamodb::Client::new(&aws_config);

    // Assumes the table already exists (PK=lease_key)
    let store = Arc::new(DynamoLeaseStore::new(dynamo, "my-leases", 10_000).await?);
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

## Example: Kinesis Shard Consumer (replacing KCL)

Each ECS Fargate task runs this. Leasy automatically distributes shards
across all running tasks, handles failover, and checkpoints progress.

```rust
use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_sdk_kinesis::types::ShardIteratorType;
use leasy::{DynamoLeaseStore, LeaseConfig, LeaseManager};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // ── AWS clients ──────────────────────────────────────────────────
    let aws_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let dynamo_client = aws_sdk_dynamodb::Client::new(&aws_config);
    let kinesis_client = aws_sdk_kinesis::Client::new(&aws_config);

    // ── Lease store + manager ────────────────────────────────────────
    let lease_duration_ms = 10_000; // 10s lease; with 3s renewal → 7s safety buffer
    
    // Assumes the table already exists (PK=lease_key)
    let store = Arc::new(DynamoLeaseStore::new(
        dynamo_client,
        "kinesis-shard-leases", 
        lease_duration_ms,
    ).await?);

    // On ECS Fargate use the task ARN for a stable worker identity.
    // Falls back to a random UUID if not on ECS.
    let worker_id = std::env::var("ECS_TASK_ARN")
        .unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());

    let config = LeaseConfig {
        lease_duration_ms: 10_000,   // must match store
        renewal_interval_ms: 3_000,  // renew every 3s
        rebalance_interval_ms: 5_000,// rebalance every 5s (+jitter)
        max_leases_per_worker: None, // no cap
    };

    let manager = Arc::new(LeaseManager::new(store, worker_id, config));

    // ── Discover shards and register as leases ───────────────────────
    let stream_name = "my-kinesis-stream";
    let mut shard_ids: Vec<String> = Vec::new();
    let mut next_token: Option<String> = None;

    loop {
        let mut req = kinesis_client.list_shards().stream_name(stream_name);
        if let Some(token) = next_token.take() {
            req = req.next_token(token);
        }
        let resp = req.send().await?;

        if let Some(shards) = resp.shards {
            for shard in &shards {
                shard_ids.push(shard.shard_id().to_string());
            }
        }

        next_token = resp.next_token().map(|s| s.to_string());
        if next_token.is_none() {
            break;
        }
    }

    shard_ids.sort(); // deterministic order across all workers
    tracing::info!("Discovered {} shards", shard_ids.len());

    for shard_id in &shard_ids {
        manager.ensure_lease(shard_id).await?;
    }

    // ── Start background renewal + rebalance loops ───────────────────
    manager.clone().start_background_tasks();

    // ── Processing loop ──────────────────────────────────────────────
    loop {
        let my_shards = manager.get_my_lease_keys().await?;
        tracing::info!("This worker owns {} shards", my_shards.len());

        for shard_id in &my_shards {
            // Resume from last checkpoint (sequence number) or start from TRIM_HORIZON
            let checkpoint = manager.get_checkpoint(shard_id).await?;

            let mut iter_req = kinesis_client
                .get_shard_iterator()
                .stream_name(stream_name)
                .shard_id(shard_id);

            iter_req = match &checkpoint {
                Some(seq) => iter_req
                    .shard_iterator_type(ShardIteratorType::AfterSequenceNumber)
                    .starting_sequence_number(seq),
                None => iter_req.shard_iterator_type(ShardIteratorType::TrimHorizon),
            };

            let iterator = iter_req.send().await?.shard_iterator;

            if let Some(iter) = iterator {
                let output = kinesis_client
                    .get_records()
                    .shard_iterator(&iter)
                    .limit(1000)
                    .send()
                    .await?;

                for record in &output.records {
                    let data = record.data().as_ref();
                    tracing::debug!(
                        "shard={} seq={} bytes={}",
                        shard_id,
                        record.sequence_number(),
                        data.len()
                    );

                    // ─── YOUR BUSINESS LOGIC HERE ───
                    // process_record(data).await?;
                }

                // Checkpoint the last sequence number we processed
                if let Some(last) = output.records.last() {
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

---

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
`false` and backs off gracefully.

### Rebalance Algorithm
```
total_leases   = 10
active_workers = 3
target_per_worker = ceil(10/3) = 4

Worker A → 4 leases
Worker B → 3 leases
Worker C → 3 leases
```
Priority order: unowned leases first, then expired leases.
Jitter on the rebalance interval prevents thundering herd on DynamoDB.

### Failure Recovery
```
Worker crashes → stops renewing → lease expires_at passes →
next rebalance on surviving workers detects expiry →
steals the lease → processing resumes from last checkpoint
```
