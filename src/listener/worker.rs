use crate::handler::{Registry, RegistryError};
use chrono::Utc;
use fx_mq_building_blocks::{models::RawMessage, queries::Queries};
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    task::{JoinError, JoinHandle},
};
use uuid::Uuid;

/// Commands scheduleable on the next iteration
pub enum WorkerCommand {
    /// Schedules a message for handling on the next iteration
    Work {
        message: RawMessage,
        confirm: oneshot::Sender<Uuid>,
    },
    /// Stops the worker loop on the next iteration
    Stop { confirm: oneshot::Sender<()> },
}

/// Errors thrown by the worker
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("Could not report idle state for worker with id {id}")]
    IdleError {
        id: usize,
        cause: mpsc::error::SendError<usize>,
    },
    #[error("Could not confirm command acception")]
    ConfirmationError,
    #[error("RegistryError: {0}")]
    RegistryError(#[from] RegistryError),
    #[error("DatabaseError: {0}")]
    Database(#[from] sqlx::Error),
}

struct Worker {
    id: usize,
    tx: mpsc::Sender<usize>,
    rx: mpsc::Receiver<WorkerCommand>,
    registry: Arc<Registry>,
    queries: Arc<Queries>,
    /// Database connection pool used by workers to report on progress
    pool: PgPool,
}

impl Worker {
    async fn new(
        id: usize,
        tx: mpsc::Sender<usize>,
        rx: mpsc::Receiver<WorkerCommand>,
        registry: Arc<Registry>,
        queries: Arc<Queries>,
        pool: PgPool,
    ) -> Result<Self, WorkerError> {
        let worker = Self {
            id,
            rx,
            tx,
            registry,
            queries,
            pool,
        };

        // Signal idle state
        worker
            .tx
            .send(worker.id)
            .await
            .map_err(|err| WorkerError::IdleError { id, cause: err })?;

        Ok(worker)
    }

    async fn work(
        &self,
        message: RawMessage,
        confirm: oneshot::Sender<Uuid>,
    ) -> Result<(), WorkerError> {
        let message_id = message.id;

        // Confirm to the caller that the message was received
        confirm
            .send(message.id)
            .map_err(|_| WorkerError::ConfirmationError)?;

        // Attempt to handle the message
        let handling_result = self.registry.handle(message).await?;

        let now = Utc::now();
        let mut tx = self.pool.begin().await?;
        match handling_result {
            Ok(_) => {
                self.queries
                    .report_success(&mut tx, message_id, now)
                    .await?;
            }
            Err(err) => {
                if err.dead {
                    self.queries
                        .report_dead(&mut tx, message_id, now, &err.error_str)
                        .await?;
                } else {
                    self.queries
                        .report_retryable(
                            &mut tx,
                            message_id,
                            now,
                            err.message_attempted,
                            err.try_earliest_at,
                            &err.error_str,
                        )
                        .await?;
                }
            }
        }
        tx.commit().await?;

        // Report idling
        self.tx
            .send(self.id)
            .await
            .map_err(|err| WorkerError::IdleError {
                id: self.id,
                cause: err,
            })?;

        Ok(())
    }
}

/// Run a worker task
async fn run_worker(mut worker: Worker) -> Result<(), WorkerError> {
    while let Some(command) = worker.rx.recv().await {
        match command {
            WorkerCommand::Work { message, confirm } => {
                if let Err(e) = worker.work(message, confirm).await {
                    tracing::error!(
                        worker_id = worker.id,
                        error = ?e,
                        "Worker failed to process message"
                    );
                    return Err(e);
                }
            }
            WorkerCommand::Stop { confirm } => {
                confirm
                    .send(())
                    .map_err(|_| WorkerError::ConfirmationError)?;

                // Exit the listening loop here. No more work will be picked up by this worker
                return Ok(());
            }
        }
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum WorkerHandleError {
    #[error("Failed to send abort command to worker")]
    SendError(#[from] mpsc::error::SendError<WorkerCommand>),
    #[error("Failed to receive abort response from worker")]
    RecvError(#[from] oneshot::error::RecvError),
    #[error("Failed to join worker task to main thread")]
    JoinError(#[from] JoinError),
}

/// Handle to a background worker that processes messages.
///
/// Provides async communication with the worker task for sending work and shutdown commands.
pub(crate) struct WorkerHandle {
    id: usize,
    tx: mpsc::Sender<WorkerCommand>,
    tx_idle: mpsc::Sender<usize>,
    join_handle: JoinHandle<Result<(), WorkerError>>,
}

impl WorkerHandle {
    /// Creates a new worker handle and spawns the background worker task.
    ///
    /// The worker immediately signals idle state and begins listening for commands.
    #[tracing::instrument(skip(tx_idle, registry, queries, pool), level = "debug")]
    pub(crate) async fn new(
        id: usize,
        tx_idle: &mpsc::Sender<usize>,
        registry: &Arc<Registry>,
        queries: &Arc<Queries>,
        pool: &PgPool,
    ) -> Result<Self, mpsc::error::SendError<usize>> {
        // Channel capacity of 1 since we track idle state
        let (tx, rx) = mpsc::channel(1);

        // Clone for move into spawned task
        let tx_idle_worker = tx_idle.clone();
        let tx_idle_handle = tx_idle.clone();
        let registry = registry.clone();
        let queries = queries.clone();
        let pool = pool.clone();

        let join_handle = tokio::spawn(async move {
            let worker = Worker::new(id, tx_idle_worker, rx, registry, queries, pool).await?;

            let result = run_worker(worker).await;

            if let Err(e) = &result {
                tracing::error!(
                    worker_id = id,
                    error = ?e,
                    "Worker task exited with error"
                );
            } else {
                tracing::info!(worker_id = id, "Worker task exited normally");
            }

            result
        });

        Ok(Self {
            id,
            tx,
            join_handle,
            tx_idle: tx_idle_handle,
        })
    }

    /// Signals that this worker is idle and available for work.
    pub(crate) async fn idle(&self) -> Result<(), mpsc::error::SendError<usize>> {
        self.tx_idle.send(self.id).await
    }

    /// Sends a message to the worker for processing.
    ///
    /// Returns the message ID once the worker confirms receipt.
    #[tracing::instrument(skip(self, message), fields(message_id = %message.id), level = "debug")]
    pub(crate) async fn work(&self, message: RawMessage) -> Result<Uuid, WorkerHandleError> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(WorkerCommand::Work {
                message,
                confirm: tx,
            })
            .await?;

        let accepted_id = rx.await?;

        Ok(accepted_id)
    }

    /// Gracefully stops the worker with a timeout, or aborts if timeout is exceeded.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum time to wait for graceful shutdown. Use `Duration::ZERO` for immediate abort.
    ///
    /// # Examples
    ///
    /// Graceful shutdown: `worker.stop_or_abort(Duration::from_secs(30)).await?`
    ///
    /// Immediate abort: `worker.stop_or_abort(Duration::ZERO).await?`
    #[tracing::instrument(skip(self), fields(timeout_ms = timeout.as_millis()), level = "debug")]
    pub(crate) async fn stop(self, timeout: Duration) -> Result<(), WorkerHandleError> {
        // Destructure to avoid ownership conflicts in tokio::select
        let WorkerHandle {
            tx, join_handle, ..
        } = self;
        let abort_handle = join_handle.abort_handle();

        tokio::select! {
            result = async move {
                // Manually implement stop() logic
                let (tx_stop, rx_stop) = oneshot::channel();
                tx.send(WorkerCommand::Stop { confirm: tx_stop }).await?;

                // await confirmation from the worker.
                rx_stop.await?;

                join_handle.await?
                    .expect("Worker should exit cleanly after Stop command");

                Ok::<(), WorkerHandleError>(())
            } => {
                // Graceful stop completed within timeout
                result
            }
            _ = tokio::time::sleep(timeout) => {
                // Timeout - abort the worker using the abort handle
                abort_handle.abort();
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use crate::{
        RegistryBuilder,
        constants::FX_MQ_JOBS_SCHEMA_NAME,
        test_tools::{
            FailingHandler, FailingHandlerMessage, LongRunningHandlerMessage, SucceedingHandler,
            SucceedingHandlerMessage, format_log_message, new_pool_publisher, new_shared_state,
            new_test_registry, new_worker_handle,
        },
    };
    use chrono::Utc;
    use fx_mq_building_blocks::{
        backoff::ConstantBackoff, migrator::run_migrations, models::Message, queries::Queries,
    };
    use uuid::Uuid;

    #[sqlx::test(migrations = false)]
    async fn it_reports_handling_success(pool: sqlx::PgPool) -> anyhow::Result<()> {
        run_migrations(&pool, FX_MQ_JOBS_SCHEMA_NAME).await?;

        let host_id = Uuid::now_v7();

        let hold_for = Duration::from_mins(1);

        let queries = Arc::new(Queries::new(FX_MQ_JOBS_SCHEMA_NAME));

        let state = new_shared_state();

        let registry = Arc::new(
            new_test_registry(&state)
                .with_lease_renewal(&queries, host_id, hold_for)
                .build(),
        );

        let (handle, mut rx_idle) = new_worker_handle(&pool, &registry).await?;

        let publisher = new_pool_publisher(&pool);

        let message = SucceedingHandlerMessage::default();

        publisher.publish(&message).await?;

        let mut message_id: Option<Uuid> = None;
        while let Some(_) = rx_idle.recv().await {
            let mut tx = pool.begin().await?;
            let polled = queries
                .get_next_unattempted(&mut tx, Utc::now(), Uuid::now_v7(), Duration::from_mins(1))
                .await?;
            tx.commit().await?;

            match polled {
                None => {
                    // Stop the worker loop
                    handle.stop(Duration::from_mins(1)).await?;
                    break;
                }
                Some(message) => {
                    message_id = Some(message.id);
                    handle.work(message).await?;
                }
            };
        }

        let guard = state.lock().await;
        assert_eq!(
            *guard,
            &[format_log_message(SucceedingHandlerMessage::NAME, 1)]
        );

        let mut tx = pool.begin().await?;
        let is_succeeded = queries
            .is_succeeded(
                &mut tx,
                message_id.expect("Expected a message_id"),
                Utc::now(),
            )
            .await?;
        tx.commit().await?;
        assert!(is_succeeded);

        Ok(())
    }

    #[sqlx::test(migrations = false)]
    async fn it_reports_handling_errors_as_retryable(pool: sqlx::PgPool) -> anyhow::Result<()> {
        run_migrations(&pool, FX_MQ_JOBS_SCHEMA_NAME).await?;

        let host_id = Uuid::now_v7();

        let hold_for = Duration::from_mins(1);

        let queries = Arc::new(Queries::new(FX_MQ_JOBS_SCHEMA_NAME));

        let state = new_shared_state();

        let registry = Arc::new(
            new_test_registry(&state)
                .with_lease_renewal(&queries, host_id, hold_for)
                .build(),
        );

        let (handle, mut rx_idle) = new_worker_handle(&pool, &registry).await?;

        let publisher = new_pool_publisher(&pool);

        let message = FailingHandlerMessage::default();

        publisher.publish(&message).await?;

        let mut message_id: Option<Uuid> = None;
        while let Some(_) = rx_idle.recv().await {
            let mut tx = pool.begin().await?;
            let polled = queries
                .get_next_unattempted(&mut tx, Utc::now(), Uuid::now_v7(), Duration::from_mins(1))
                .await?;
            tx.commit().await?;

            match polled {
                None => {
                    // Stop the worker loop
                    handle.stop(Duration::from_mins(1)).await?;
                    break;
                }
                Some(message) => {
                    message_id = Some(message.id);
                    handle.work(message).await?;
                }
            };
        }

        let guard = state.lock().await;
        assert_eq!(
            *guard,
            &[format_log_message(FailingHandlerMessage::NAME, 1)]
        );

        let mut tx = pool.begin().await?;
        let is_failed = queries
            .is_failed(
                &mut tx,
                message_id.expect("Expected a message_id"),
                Utc::now(),
            )
            .await?;
        tx.commit().await?;
        assert!(is_failed);

        Ok(())
    }

    #[sqlx::test(migrations = false)]
    async fn it_reports_handling_errors_as_dead(pool: sqlx::PgPool) -> anyhow::Result<()> {
        run_migrations(&pool, FX_MQ_JOBS_SCHEMA_NAME).await?;

        let host_id = Uuid::now_v7();

        let hold_for = Duration::from_mins(1);

        let queries = Arc::new(Queries::new(FX_MQ_JOBS_SCHEMA_NAME));

        let state = new_shared_state();

        let base_delay = Duration::from_millis(5);

        let succeeding_handler =
            SucceedingHandler::new(state.clone(), ConstantBackoff::new(base_delay), 3);

        let failing_handler =
            FailingHandler::new(state.clone(), ConstantBackoff::new(base_delay), 3);

        let registry = Arc::new(
            RegistryBuilder::default()
                .with_handler(succeeding_handler)
                .with_handler(failing_handler)
                .with_lease_renewal(&queries, host_id, hold_for)
                .build(),
        );

        let (handle, mut rx_idle) = new_worker_handle(&pool, &registry).await?;

        let publisher = new_pool_publisher(&pool);

        let message = FailingHandlerMessage::default();

        publisher.publish(&message).await?;

        let mut message_id: Option<Uuid> = None;
        while let Some(_) = rx_idle.recv().await {
            let mut tx = pool.begin().await?;
            // Poll for unattempted messages
            let unattempted = queries
                .get_next_unattempted(
                    &mut tx,
                    Utc::now(),
                    Uuid::now_v7(),
                    Duration::from_mins(1), // leases should be cleared on failure
                )
                .await?;

            // Poll for retryable messages
            let retryable = queries
                .get_next_retryable(
                    &mut tx,
                    Utc::now(),
                    Uuid::now_v7(),
                    Duration::from_mins(1), // leases should be cleared on failure
                )
                .await?;
            tx.commit().await?;

            match (unattempted, retryable) {
                (Some(unattempted), None) => {
                    message_id = Some(unattempted.id);
                    handle.work(unattempted).await?;
                }
                (None, Some(retryable)) => {
                    message_id = Some(retryable.id);
                    handle.work(retryable).await?;
                }
                (None, None) => {
                    handle.stop(Duration::from_mins(1)).await?;
                    break;
                }
                (Some(_), Some(_)) => panic!(
                    "Received both unattempted and retryable. Message is in two states at once!"
                ),
            }

            // Sleep for the base delay of the constant backoff
            tokio::time::sleep(base_delay).await;
        }

        let guard = state.lock().await;
        // Expect the failing handler to have been called thrice
        assert_eq!(
            *guard,
            &[
                format_log_message(FailingHandlerMessage::NAME, 1),
                format_log_message(FailingHandlerMessage::NAME, 2),
                format_log_message(FailingHandlerMessage::NAME, 3)
            ]
        );

        let mut tx = pool.begin().await?;
        // Expect the message to be in dead state
        let is_dead = queries
            .is_dead(
                &mut tx,
                message_id.expect("Expected a message_id"),
                Utc::now(),
            )
            .await?;
        tx.commit().await?;
        assert!(is_dead);

        Ok(())
    }

    #[sqlx::test(migrations = false)]
    async fn it_aborts_handling(pool: sqlx::PgPool) -> anyhow::Result<()> {
        run_migrations(&pool, FX_MQ_JOBS_SCHEMA_NAME).await?;

        let host_id = Uuid::now_v7();

        let hold_for = Duration::from_mins(1);

        let queries = Arc::new(Queries::new(FX_MQ_JOBS_SCHEMA_NAME));

        let state = new_shared_state();

        let registry = Arc::new(
            new_test_registry(&state)
                .with_lease_renewal(&queries, host_id, hold_for)
                .build(),
        );

        let (handle, mut rx_idle) = new_worker_handle(&pool, &registry).await?;

        let publisher = new_pool_publisher(&pool);

        let message = LongRunningHandlerMessage::default();

        publisher.publish(&message).await?;

        let queries = Queries::new(FX_MQ_JOBS_SCHEMA_NAME);

        let mut message_id: Option<Uuid> = None;

        let hold_for = Duration::from_millis(50);

        if let Some(_) = rx_idle.recv().await {
            let mut tx = pool.begin().await?;
            let message = queries
                .get_next_unattempted(&mut tx, Utc::now(), Uuid::now_v7(), hold_for)
                .await?
                .expect("Expected a message to be polled but received None");
            tx.commit().await?;

            message_id = Some(message.id);

            // This sends work to the worker task. It awaits confirmation, but does not wait for the work to be done
            handle.work(message).await?;

            // Aborts the worker task without waiting for completion by dropping its join handle
            handle.stop(Duration::ZERO).await?;
        }

        // Expect no state to be pushed yet
        let guard = state.lock().await;
        assert!(guard.is_empty());

        // Expect the attempt to be in progress for the duration of the lease.
        let mut tx = pool.begin().await?;
        let is_in_progress = queries
            .is_in_progress(
                &mut tx,
                message_id.expect("Expected a message_id"),
                Utc::now(),
            )
            .await?;
        tx.commit().await?;
        assert!(is_in_progress);

        // Sleep for the duration of the lease
        tokio::time::sleep(hold_for).await;

        // Then expect the attempt to be missing
        let mut tx = pool.begin().await?;
        let is_missing = queries
            .is_missing(
                &mut tx,
                message_id.expect("Expected a message_id"),
                Utc::now(),
            )
            .await?;
        tx.commit().await?;
        assert!(is_missing);

        Ok(())
    }
}
