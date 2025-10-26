# fx-mq-jobs
A type-safe job queue for monolithic Rust applications. Treats jobs as **commands** that run in the background with retries and crash recovery using a lease-based model.

Built for deployments where every host can handle all job types - no separate queues or specialized workers needed.

## What it is
- **Persistent and durable** - jobs survive application restarts and are retried until successful or marked dead.
- Uses PostgreSQL FOR UPDATE SKIP LOCKED for concurrent processing and at-least-once delivery.
- **Type-safe message routing** - handlers are registered at compile time with strong type guarantees.
- **Lease-based processing** - handlers can extend processing time for long-running tasks.
- **Priority scheduling** - new jobs → retryable jobs → recovered jobs (crashed workers).
- **Graceful shutdown** with configurable timeouts and worker coordination.

## What it is not
- Not an event bus. Jobs are **commands** (imperative) that execute specific actions, not **events** (declarative) that multiple handlers observe.

## Usage
**IMPORTANT** - Running blocking code within job handlers may starve the listener. If you need to run blocking jobs in your handler, ensure the listener runs in a separate thread. Blocking the listener will cause long running transactions, and will trigger sqlx slow query warnings in the logs.

### Configure the listener
```rust
use fx_jobs::{Handler, RegistryBuilder, Listener, LeaseRenewer};

// 1. Define a job message
#[derive(Serialize, Deserialize, Clone)]
struct SendEmailJob {
    to: String,
    subject: String,
    body: String,
}

impl Message for SendEmailJob {
    const NAME: &'static str = "send_email";
}

// 2. Implement handler (one handler per job type)
struct EmailHandler;

impl Handler for EmailHandler {
    type Message = SendEmailJob;
    type Error = EmailError;

    fn handle<'a>(&'a self, job: Self::Message, lease_renewer: LeaseRenewer) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move {
            // For long-running jobs, extend the lease
            // lease_renewer.renew_lease(Utc::now(), &pool).await?;

            send_email(&job.to, &job.subject, &job.body).await?;
            Ok(())
        })
    }

    fn try_at(&self, attempted: i32, attempted_at: DateTime<Utc>) -> DateTime<Utc> {
        attempted_at + Duration::from_secs(60 * attempted as u64) // Linear backoff
    }

    fn max_attempts(&self) -> i32 { 3 }
}

// 3. Set up processing
let registry = RegistryBuilder::new()
    .with_handler(EmailHandler);

let mut listener = Listener::new(
    pool,
    registry,
    4,                     // worker count
    host_id,               // uuid of this host
    Duration::from_mins(5) // job lease duration
).await?;

// 4. Start processing
listener.listen().await?;
```

### Publishing Jobs

```rust
let publisher = Publisher::new(&pool, &queries);
let job = SendEmailJob { /* ... */ };
publisher.publish(&job).await?;
```

## Key Concepts

- **Jobs are commands**: Each job type has exactly one handler that executes a specific action.
- **Lease-based processing**: Workers hold exclusive leases on jobs; crashed workers' jobs are recovered after their lease expires.
- **Type safety**: Registered handlers have compile-time type guarantees. Unregistered message types cause worker failures.
- **Retry logic**: Failed jobs are automatically retried with configurable backoff strategies.
- **Priority scheduling**: Ensures new work isn't starved by failing jobs.

## Requirements
- PostgreSQL
- Rust 1.70+
