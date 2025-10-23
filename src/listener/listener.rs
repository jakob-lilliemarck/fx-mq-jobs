use crate::{
    constants::FX_MQ_JOBS_SCHEMA_NAME,
    handler::RegistryBuilder,
    listener::worker::{WorkerHandle, WorkerHandleError},
};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use fx_mq_building_blocks::{
    backoff::ExponentialBackoff, constants::FX_MQ_MESSAGE_NOTIFICATION_CHANNEL,
    listener::PollControlStream, queries::Queries,
};
use sqlx::{PgPool, postgres::PgListener};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum ListenerError {
    #[error("DatabaseError {0}")]
    Database(#[from] sqlx::Error),
    #[error("IdleChannel missing")]
    IdleChannel,
    #[error("WorkerHandleError: {0}")]
    WorkerHandleError(#[from] WorkerHandleError),
}

/// Job listener that coordinates workers and database polling.
///
/// Manages worker pool, database polling with exponential backoff, and PostgreSQL notifications.
///
/// `Listener` is intended to be instantiated once per host machine. Use the `worker` argument to control the number of worker tasks that consume messages from the listener.
///
/// # Example
///
/// ```rust,no_run
/// # use fx_mq_jobs::Listener;
/// # use std::time::Duration;
/// # use uuid::Uuid;
/// #
/// # async fn example() -> anyhow::Result<()> {
/// #     let pool = todo!();
/// #     let registry = todo!();
/// #     let host_id = Uuid::now_v7();
/// #     let hold_for = Duration::from_mins(1);
/// #
/// let mut listener = Listener::new(
///     pool,
///     registry,
///     4,
///     host_id,
///     hold_for
/// ).await?;
///
/// listener.listen().await?;
/// #     Ok(())
/// # }
/// ```
pub struct Listener {
    /// Database connection pool used to poll for new messages.
    pool: PgPool,
    /// Receiver for worker idling messages
    rx_idle: mpsc::Receiver<usize>,
    /// Worker handles
    handles: Vec<WorkerHandle>,
    /// Queries namespaced with schema
    queries: Arc<Queries>,
    /// The host id. Use an id thats static across restarts for better observability
    host_id: Uuid,
    /// The duration workers lease messages for
    hold_for: Duration,
}

impl Listener {
    /// Creates a new job listener with the specified worker pool.
    ///
    /// # Arguments
    ///
    /// * `workers` - Number of worker threads to spawn
    /// * `host_id` - Static host identifier for observability
    /// * `hold_for` - Duration workers lease messages
    ///
    /// # Returns
    ///
    /// Configured listener ready to process jobs.
    #[tracing::instrument(
        skip(pool, registry_builder),
        fields(workers = workers, host_id = %host_id),
        level = "debug"
    )]
    pub async fn new(
        pool: PgPool,
        registry_builder: RegistryBuilder,
        workers: usize,
        host_id: Uuid,
        hold_for: Duration,
    ) -> Result<Self, mpsc::error::SendError<usize>> {
        let queries = Arc::new(Queries::new(FX_MQ_JOBS_SCHEMA_NAME));
        let registry = Arc::new(
            registry_builder
                .with_lease_renewal(&queries, host_id, hold_for)
                .build(),
        );

        // Create channel for workers to signal when they become idle
        let (tx_idle, rx_idle) = mpsc::channel(workers);

        // Spawn worker handles that will process jobs asynchronously
        let mut handles = Vec::with_capacity(workers);
        for id in 0..workers {
            let handle = WorkerHandle::new(id, &tx_idle, &registry, &queries, &pool).await?;

            handles.push(handle);
        }

        Ok(Self {
            pool,
            rx_idle,
            handles,
            queries,
            host_id,
            hold_for,
        })
    }

    /// Starts the job processing loop.
    ///
    /// Listens for PostgreSQL notifications and polls for jobs with exponential backoff.
    /// Runs indefinitely until an unrecoverable error occurs.
    ///
    /// # Errors
    ///
    /// Returns error if database connection fails or worker coordination breaks.
    #[tracing::instrument(
        skip(self),
        fields(host_id = %self.host_id),
        level = "debug"
    )]
    pub async fn listen(&mut self) -> Result<(), ListenerError> {
        // Set up polling control with exponential backoff (base 2, 2 second initial delay)
        let mut control =
            PollControlStream::new(ExponentialBackoff::new(2, Duration::from_secs(2)));

        // Set up PostgreSQL LISTEN/NOTIFY for immediate job notifications
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen(FX_MQ_MESSAGE_NOTIFICATION_CHANNEL).await?;
        let pg_stream = listener.into_stream();

        control.with_pg_stream(pg_stream);

        while let Some(result) = control.next().await {
            if let Err(err) = result {
                tracing::warn!(message="The control stream returned an error to the job listener", err=?err);
            }

            // Get a new timestamp
            let now = Utc::now();

            // Poll using the current timestamp
            match self.poll(now).await {
                Ok(accepted_message_id) => {
                    // Reset control stream failures on success
                    control.reset_failed_attempts();

                    // If we successfully handed off a job, poll immediately for more work
                    if let Some(_) = accepted_message_id {
                        control.set_poll();
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        message="Polling for jobs returned an error",
                        error=?err
                    );

                    // Backoff on any polling error to avoid overwhelming the system
                    control.increment_failed_attempts();
                }
            };
        }
        Ok(())
    }

    /// Gracefully stops all workers with a timeout.
    ///
    /// Sends stop commands to all worker handles and waits for them to complete.
    /// If workers don't stop within the timeout, they will be forcibly aborted.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum time to wait for graceful shutdown
    ///
    /// # Errors
    ///
    /// Returns error if worker shutdown fails or times out.
    #[tracing::instrument(
        skip(self),
        fields(workers = self.handles.len(), timeout_secs = timeout.as_secs()),
        level = "info"
    )]
    pub async fn stop(self, timeout: Duration) -> Result<(), ListenerError> {
        // Use the new stop_or_abort method for each worker
        let stop_futures: Vec<_> = self.handles.into_iter().map(|h| h.stop(timeout)).collect();

        // Wait for all workers to stop (gracefully or via abort)
        let results = futures::future::try_join_all(stop_futures).await;

        match results {
            Ok(_) => {
                tracing::info!("All workers stopped (gracefully or via abort)");
                Ok(())
            }
            Err(e) => {
                tracing::error!("Worker shutdown failed: {}", e);
                Err(ListenerError::WorkerHandleError(e))
            }
        }
    }

    /// Polls for a single job and assigns it to an idle worker.
    ///
    /// # Arguments
    ///
    /// * `now` - Current timestamp for lease calculations
    ///
    /// # Returns
    ///
    /// ID of accepted message, or `None` if no jobs available or no idle workers.
    ///
    /// # Errors
    ///
    /// Returns error if database operations fail or worker rejects the job.
    #[tracing::instrument(
        skip(self),
        fields(host_id = %self.host_id),
        level = "debug"
    )]
    async fn poll(&mut self, now: DateTime<Utc>) -> Result<Option<Uuid>, ListenerError> {
        // Wait for an idle worker to become available
        let idle_worker_idx = match self.rx_idle.recv().await {
            None => {
                // Channel closed - all workers exited
                tracing::error!("All workers disconnected");
                return Err(ListenerError::IdleChannel);
            }
            Some(idle) => idle,
        };

        // Get the worker handle
        let worker = self
            .handles
            .get(idle_worker_idx)
            .expect("Getting handle by index returned None");

        // Query database for messages
        let mut tx = self.pool.begin().await?;

        // Priority 1: Brand new jobs that haven't been attempted yet (fastest to process)
        let message = if let Some(message) = self
            .queries
            .get_next_unattempted(&mut tx, now, self.host_id, self.hold_for)
            .await?
        {
            message
        // Priority 2: Jobs that failed but are ready to retry (within retry limits)
        } else if let Some(message) = self
            .queries
            .get_next_retryable(&mut tx, now, self.host_id, self.hold_for)
            .await?
        {
            message
        // Priority 3: Jobs with expired leases (likely from crashed workers)
        } else if let Some(message) = self
            .queries
            .get_next_missing(&mut tx, now, self.host_id, self.hold_for)
            .await?
        {
            message
        } else {
            // No jobs available - return worker to idle pool
            if let Err(e) = worker.idle().await {
                tracing::error!("Failed to return worker to idle pool: {}", e);
            }

            return Ok(None);
        };

        // Wait for the worker to accept the message (this only wait for the worker to confirm, not for the entire work duration)
        let message_id = worker.work(message).await?;

        // Commit the transaction after we know the worker accepted the message
        tx.commit().await?;

        Ok(Some(message_id))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Listener, Publisher,
        constants::FX_MQ_JOBS_SCHEMA_NAME,
        test_tools::{
            FailingOnFirstCallHandlerMessage, SucceedingHandlerMessage, format_log_message,
            new_shared_state, new_test_registry,
        },
    };
    use chrono::Utc;
    use fx_mq_building_blocks::{migrator::run_migrations, models::Message, queries::Queries};
    use sqlx::PgPool;
    use std::{sync::Arc, time::Duration};
    use tokio::sync::oneshot;
    use uuid::Uuid;

    #[sqlx::test(migrations = false)]
    async fn it_works_on_messages(pool: sqlx::PgPool) -> anyhow::Result<()> {
        run_migrations(&pool, FX_MQ_JOBS_SCHEMA_NAME).await?;

        let now = Utc::now();

        let host_id = Uuid::now_v7();

        let hold_for = Duration::from_mins(1);

        let state = new_shared_state();

        let registry = new_test_registry(&state);

        let queries = Arc::new(Queries::new(FX_MQ_JOBS_SCHEMA_NAME));

        let publisher = Publisher::<PgPool>::new(&pool, &queries);

        let succeeding = SucceedingHandlerMessage::default();

        let failing = FailingOnFirstCallHandlerMessage::default();

        // Publish and move a message along to simulate missing state
        let published_1 = publisher.publish(&succeeding).await?;
        let mut tx = pool.begin().await?;
        queries
            .get_next_unattempted(&mut tx, now, host_id, Duration::from_secs(0))
            .await?;
        // Assert its in the expected state
        assert!(queries.is_missing(&mut tx, published_1.id, now).await?);
        tx.commit().await?;

        // Publish a message that will fail on the first attempt and then succeed
        let _published_2 = publisher.publish(&failing).await?;

        // Publish a message that will succeed directly
        let _published_3 = publisher.publish(&succeeding).await?;

        let mut listener = Listener::new(pool, registry, 1, host_id, hold_for).await?;

        let (tx_stop, rx_stop) = oneshot::channel::<()>();
        let listen_handle = tokio::spawn(async move {
            tokio::select! {
                result = listener.listen() => result,
                _ = rx_stop => {
                    // it should be perfectly safe to abort here as we have already waited for 4 handle calls and we expect no more.
                    listener.stop(Duration::ZERO).await?;
                    Ok(())
                }
            }
        });

        loop {
            let guard = state.lock().await;

            // We expect exactly 4 handler calls
            if guard.len() == 4 {
                break;
            }
        }

        if let Err(_) = tx_stop.send(()) {
            panic!("tx_stop returned an error");
        }

        listen_handle.await??;
        let guard = state.lock().await;
        // Expect the failing handler to have been called thrice
        assert_eq!(
            *guard,
            &[
                // this message is handled first because it is published first out of the two unattempted messages
                format_log_message(FailingOnFirstCallHandlerMessage::NAME, 1),
                // this message is handled second because it is published second out of the two unattempted messages
                format_log_message(SucceedingHandlerMessage::NAME, 1),
                // this is handled third as retryable messages have higher priority than missing
                format_log_message(FailingOnFirstCallHandlerMessage::NAME, 2),
                // this is handled last as missing has the lowest priority
                format_log_message(SucceedingHandlerMessage::NAME, 2)
            ]
        );

        Ok(())
    }
}
