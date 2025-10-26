use fx_mq_jobs::{Handler, Message};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{env, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
#[error("HelloWorldError")]
pub struct HelloWorldError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloWorldMessage {
    pub message: String,
}

impl Message for HelloWorldMessage {
    const NAME: &str = "GenerateInitialPopulationMessage";
}

/// Handler that processes initial population generation jobs.
pub struct GenerateInitialPopulationHandler {
    handled: Arc<Mutex<bool>>,
}

impl Handler for GenerateInitialPopulationHandler {
    type Message = HelloWorldMessage;
    type Error = HelloWorldError;

    fn handle<'a>(
        &'a self,
        message: Self::Message,
        _: fx_mq_jobs::LeaseRenewer,
    ) -> futures::future::BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move {
            println!("Received: {:?}", message);
            let mut flag = self.handled.lock().await;
            *flag = true;
            Ok(())
        })
    }

    fn max_attempts(&self) -> i32 {
        5
    }

    fn try_at(
        &self,
        _: i32,
        attempted_at: chrono::DateTime<chrono::Utc>,
    ) -> chrono::DateTime<chrono::Utc> {
        attempted_at + Duration::from_secs(5)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Database setup - genetic algorithms need persistent storage for populations
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    fx_mq_building_blocks::migrator::run_migrations(&pool, fx_mq_jobs::FX_MQ_JOBS_SCHEMA_NAME)
        .await?;

    // Some shared state - this would typically be a database. Using a Arc<Mutex<bool>> for simplicity
    let handled = Arc::new(Mutex::new(false));

    // Create a job handler
    let job_handler = GenerateInitialPopulationHandler {
        handled: handled.clone(),
    };

    // Set up a job registry, register the handler
    let job_registry = fx_mq_jobs::RegistryBuilder::new().with_handler(job_handler);

    // Create an ID to identify this host machine.
    let host_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("valid uuid");

    // Specify the lease period. This is the time a message is held exclusivly by a worker.
    // The lease period may be renewed by the worker using the LeaseRenewer. Doing so will renew
    // the lease with this duration
    let hold_for = Duration::from_secs(2);

    // Create the listener, passing the job registry
    let mut listener =
        fx_mq_jobs::Listener::new(pool.clone(), job_registry, 2, host_id, hold_for).await?;

    // Move the listener to a background task and start listening
    tokio::spawn(async move {
        listener.listen().await?;
        Ok::<(), anyhow::Error>(())
    });

    // Create a query struct. This is essentially just a way to control which schema queries run against,
    // making it possible to re-use the database schema to run separate listeners etc.
    let queries = Arc::new(fx_mq_jobs::Queries::new(fx_mq_jobs::FX_MQ_JOBS_SCHEMA_NAME));

    // Create a job publisher.
    let publisher = fx_mq_jobs::Publisher::<PgPool>::new(&pool, &queries);

    // Publish a job
    let _published = publisher
        .publish(&HelloWorldMessage {
            message: "Hello world".to_string(),
        })
        .await?;

    loop {
        let flag = handled.lock().await;
        if *flag {
            break;
        }
    }

    Ok(())
}
