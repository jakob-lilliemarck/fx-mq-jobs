use crate::lease_renewer::LeaseRenewer;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use fx_mq_building_blocks::{
    models::{Message, RawMessage},
    queries::Queries,
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use uuid::Uuid;

/// Trait for processing typed messages with retry logic.
///
/// Implementors define how to process specific message types, including retry behavior
/// and maximum attempt limits.
pub trait Handler: Send + Sync {
    type Message: Message;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Processes a message of the specified type.
    ///
    /// # Arguments
    /// * `message` - The typed message to process
    /// * `lease_renewer` - Utility to renew the message lease during processing
    ///
    /// # Returns
    /// `Ok(())` if processing succeeds, `Err(Self::Error)` if it fails and should be retried.
    fn handle<'a>(
        &'a self,
        message: Self::Message,
        lease_renewer: LeaseRenewer,
    ) -> BoxFuture<'a, Result<(), Self::Error>>;

    /// Calculates when to retry a failed message.
    ///
    /// Implementors can use backoff utilities from `fx-mq-building-blocks`
    /// (ExponentialBackoff, ConstantBackoff, LinearBackoff) or implement custom logic.
    ///
    /// # Arguments
    /// * `attempted` - Number of attempts already made
    /// * `attempted_at` - When the last attempt was made
    ///
    /// # Returns
    /// The timestamp when the next retry should occur.
    fn try_at(&self, attempted: i32, attempted_at: DateTime<Utc>) -> DateTime<Utc>;

    /// Maximum number of retry attempts before marking as dead.
    fn max_attempts(&self) -> i32;
}

trait ErasedHandler: Send + Sync {
    fn handle_erased(
        &self,
        message: RawMessage,
        lease_renewer: LeaseRenewer,
    ) -> BoxFuture<'_, Result<(), String>>;

    fn try_at(&self, attempted: i32, attempted_at: DateTime<Utc>) -> DateTime<Utc>;

    fn max_attempts(&self) -> i32;
}

impl<H> ErasedHandler for H
where
    H: Handler,
{
    #[tracing::instrument(
        skip(self, message, lease_renewer),
        fields(
            message_id = %message.id,
            message_name = %message.name,
            hash = message.hash,
            attempted = message.attempted
        ),
        level = "debug",
        err
    )]
    fn handle_erased(
        &self,
        message: RawMessage,
        lease_renewer: LeaseRenewer,
    ) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            // Deserialize raw message to typed message
            let typed: H::Message = match serde_json::from_value(message.payload) {
                Ok(typed) => typed,
                Err(err) => {
                    return Err(err.to_string());
                }
            };

            // Process with typed handler
            self.handle(typed, lease_renewer)
                .await
                .map_err(|err| err.to_string())
        })
    }

    #[tracing::instrument(
        skip(self),
        fields(attempted, attempted_at = %attempted_at),
        level = "debug",
        ret
    )]
    fn try_at(&self, attempted: i32, attempted_at: DateTime<Utc>) -> DateTime<Utc> {
        self.try_at(attempted, attempted_at)
    }

    fn max_attempts(&self) -> i32 {
        self.max_attempts()
    }
}

/// Error returned when no handler is registered for a message type.
///
/// This indicates that the registry cannot route the message because
/// no handler was registered for the message's hash.
#[derive(Debug, thiserror::Error)]
#[error("RegistryError: No handler registered for name: {name} (hash: {hash})")]
pub struct RegistryError {
    pub hash: i32,
    pub name: String,
}

/// Error returned when a handler fails to process a message.
///
/// Contains retry information and indicates whether the message should be
/// retried or marked as dead based on attempt limits.
#[derive(Debug, thiserror::Error)]
#[error(
    "HandlerError: Failed to handle message with with {message_id} (attempted: {message_attempted}"
)]
pub struct HandlerError {
    /// The ID of the message that failed to process
    pub message_id: Uuid,
    /// Number of attempts made to process this message
    pub message_attempted: i32,
    /// Whether the message has exceeded max attempts and should be marked dead
    pub dead: bool,
    /// When the message should be retried (if not dead)
    pub try_earliest_at: DateTime<Utc>,
    /// The error message from the handler
    pub error_str: String,
}

/// Builder for creating a registry with typed handlers.
///
/// Use this builder to register handlers for different message types, then pass
/// it to `Listener::new()` which will complete the registry construction internally.
///
/// # Example
///
/// ```no_run
/// # use fx_mq_jobs::{RegistryBuilder, Handler, Listener, LeaseRenewer};
/// # use fx_mq_building_blocks::models::Message;
/// # use futures::future::BoxFuture;
/// # use std::time::Duration;
/// # use chrono::{DateTime, Utc};
/// # use serde::{Serialize, Deserialize};
/// # use sqlx::PgPool;
/// # use uuid::Uuid;
/// #
/// # #[derive(Serialize, Deserialize, Clone)]
/// # struct EmailMessage { to: String, subject: String }
/// # impl Message for EmailMessage {
/// #     const NAME: &'static str = "email";
/// #     const HASH: i32 = 12345;
/// # }
/// #
/// struct EmailHandler;
///
/// impl Handler for EmailHandler {
///     type Message = EmailMessage;
///     type Error = std::io::Error;
///
///     fn handle<'a>(&'a self, message: Self::Message, _lease_renewer: LeaseRenewer) -> BoxFuture<'a, Result<(), Self::Error>> {
///         Box::pin(async move {
///             // Your message processing logic goes here
///             // Use _lease_renewer.renew_lease(now, pool).await if needed
///             Ok(())
///         })
///     }
///
///     fn try_at(&self, attempted: i32, attempted_at: DateTime<Utc>) -> DateTime<Utc> {
///         attempted_at + Duration::from_secs(60) // Retry after 1 minute
///     }
///
///     fn max_attempts(&self) -> i32 {
///         3
///     }
/// }
///
/// # async fn example(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
/// // Build registry with typed handlers
/// let registry_builder = RegistryBuilder::new()
///     .with_handler(EmailHandler);
///
/// // Pass builder to listener which completes the registry internally
/// let mut listener = Listener::new(
///     pool,
///     registry_builder,
///     4,                     // worker count
///     Uuid::now_v7(),        // host_id
///     Duration::from_mins(5) // hold_for
/// ).await?;
///
/// // Start processing jobs
/// listener.listen().await?;
/// # Ok(())
/// # }
/// ```
pub struct RegistryBuilder {
    handlers: HashMap<i32, Box<dyn ErasedHandler>>,
    queries: Option<Arc<Queries>>,
    host_id: Option<Uuid>,
    hold_for: Option<Duration>,
}

// FIXME - we should implement a type-safe builder for thisone!
impl Default for RegistryBuilder {
    fn default() -> Self {
        Self {
            handlers: HashMap::new(),
            queries: None,
            host_id: None,
            hold_for: None,
        }
    }
}

impl RegistryBuilder {
    /// Creates a new registry builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a typed handler for a specific message type.
    ///
    /// If a handler is already registered for this message type, it will be overwritten
    /// with a warning. Jobs are commands and should have exactly one handler.
    pub fn with_handler<M, H>(mut self, handler: H) -> Self
    where
        M: Message,
        H: Handler<Message = M> + 'static,
    {
        if let Some(_) = self.handlers.insert(M::HASH, Box::new(handler)) {
            tracing::warn!(
                "A handler was registered more than once for {} with hash {}",
                M::NAME,
                M::HASH
            );
        }
        self
    }

    /// Sets the queries, host_id, and hold_for parameters for lease renewal.
    pub(crate) fn with_lease_renewal(
        mut self,
        queries: &Arc<Queries>,
        host_id: Uuid,
        hold_for: Duration,
    ) -> Self {
        self.queries = Some(queries.clone());
        self.host_id = Some(host_id);
        self.hold_for = Some(hold_for);
        self
    }

    /// Builds the registry with all registered handlers.
    pub(crate) fn build(self) -> Registry {
        Registry {
            handlers: self.handlers,
            queries: self
                .queries
                .expect("queries must be set with with_lease_renewal"),
            host_id: self
                .host_id
                .expect("host_id must be set with with_lease_renewal"),
            hold_for: self
                .hold_for
                .expect("hold_for must be set with with_lease_renewal"),
        }
    }
}

/// Registry that routes raw messages to their appropriate typed handlers.
///
/// This is an internal implementation detail - users interact with `RegistryBuilder`.
pub(crate) struct Registry {
    handlers: HashMap<i32, Box<dyn ErasedHandler>>,
    queries: Arc<Queries>,
    host_id: Uuid,
    hold_for: Duration,
}

impl Registry {
    /// Routes a raw message to its registered handler.
    ///
    /// This method is used internally by workers and should not be called directly by client code.
    ///
    /// # Arguments
    /// * `message` - The raw message to process
    ///
    /// # Returns
    /// `Ok(Ok(()))` if handling succeeds, `Ok(Err(HandlerError))` if it fails but should be retried,
    /// or `Err(RegistryError)` if no handler is registered for this message type.
    #[tracing::instrument(skip(self, message), fields(message_id = %message.id, message_name = %message.name, hash = message.hash), level = "debug")]
    pub(crate) async fn handle(
        &self,
        message: RawMessage,
    ) -> Result<Result<(), HandlerError>, RegistryError> {
        match self.handlers.get(&message.hash) {
            Some(handler) => {
                let now = Utc::now(); // Current time for retry scheduling
                let message_id = message.id;
                let attemped = message.attempted + 1;
                let is_dead = attemped >= handler.max_attempts();

                // Create lease renewer for this specific message
                let lease_renewer = LeaseRenewer::new(
                    self.queries.clone(),
                    message.id,
                    self.host_id,
                    self.hold_for,
                );

                Ok(handler
                    .handle_erased(message, lease_renewer)
                    .await
                    .map_err(|error_str| HandlerError {
                        message_id,
                        message_attempted: attemped,
                        dead: is_dead,
                        try_earliest_at: handler.try_at(attemped, now),
                        error_str,
                    }))
            }
            // Jobs (unlike events) require a registered handler
            None => Err(RegistryError {
                hash: message.hash,
                name: message.name,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        constants::FX_MQ_JOBS_SCHEMA_NAME,
        test_tools::{
            FailingHandler, FailingHandlerMessage, SucceedingHandler, SucceedingHandlerMessage,
            format_log_message, new_shared_state,
        },
    };
    use fx_mq_building_blocks::{backoff::ConstantBackoff, migrator::run_migrations};
    use std::{sync::Arc, time::Duration};
    use tokio::sync::Mutex;

    #[sqlx::test(migrations = false)]
    async fn it_successfully_handles_a_message(pool: sqlx::PgPool) -> anyhow::Result<()> {
        run_migrations(&pool, FX_MQ_JOBS_SCHEMA_NAME).await?;

        let host_id = Uuid::now_v7();

        let hold_for = Duration::from_mins(1);

        let queries = Arc::new(Queries::new(FX_MQ_JOBS_SCHEMA_NAME));

        let state = new_shared_state();

        let registry = RegistryBuilder::default()
            .with_handler(SucceedingHandler {
                state: state.clone(),
                backoff: ConstantBackoff::new(Duration::from_mins(1)),
                max_attempts: 3,
                called: Arc::new(Mutex::new(0)),
            })
            .with_lease_renewal(&queries, host_id, hold_for)
            .build();

        let raw = SucceedingHandlerMessage::default().to_raw()?;

        let result = registry.handle(raw).await?;

        assert_eq!(
            &*state.lock().await,
            &[format_log_message(SucceedingHandlerMessage::NAME, 1)]
        );
        assert!(result.is_ok());
        Ok(())
    }

    #[sqlx::test(migrations = false)]
    async fn it_returns_a_handler_error_when_handling_fails(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        run_migrations(&pool, FX_MQ_JOBS_SCHEMA_NAME).await?;

        let host_id = Uuid::now_v7();

        let hold_for = Duration::from_mins(1);

        let queries = Arc::new(Queries::new(FX_MQ_JOBS_SCHEMA_NAME));

        let state = new_shared_state();

        let registry = RegistryBuilder::default()
            .with_handler(FailingHandler {
                state: state.clone(),
                backoff: ConstantBackoff::new(Duration::from_mins(1)),
                max_attempts: 3,
                called: Arc::new(Mutex::new(0)),
            })
            .with_lease_renewal(&queries, host_id, hold_for)
            .build();

        let raw = FailingHandlerMessage::default().to_raw()?;

        let message_id = raw.id;

        let result = registry.handle(raw).await?;

        assert_eq!(
            &*state.lock().await,
            &[format_log_message(FailingHandlerMessage::NAME, 1)]
        );
        if let Err(handler_error) = result {
            assert_eq!(handler_error.message_id, message_id);
            assert_eq!(handler_error.message_attempted, 1);
            assert_eq!(handler_error.dead, false);
        } else {
            panic!("Expected HandlerError");
        }
        Ok(())
    }

    #[sqlx::test(migrations = false)]
    async fn it_returns_registry_error_for_unhandled_message(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        run_migrations(&pool, FX_MQ_JOBS_SCHEMA_NAME).await?;

        let host_id = Uuid::now_v7();
        let hold_for = Duration::from_mins(1);
        let queries = Arc::new(Queries::new(FX_MQ_JOBS_SCHEMA_NAME));

        // Create a registry with only the succeeding handler - no handler for UnhandledMessage
        let registry = RegistryBuilder::default()
            .with_handler(SucceedingHandler {
                state: new_shared_state(),
                backoff: ConstantBackoff::new(Duration::from_mins(1)),
                max_attempts: 3,
                called: Arc::new(Mutex::new(0)),
            })
            .with_lease_renewal(&queries, host_id, hold_for)
            .build();

        // Create a raw message for UnhandledMessage type
        use crate::test_tools::UnhandledMessage;
        let unhandled_message = UnhandledMessage::default();
        let raw_message = RawMessage {
            id: Uuid::now_v7(),
            name: UnhandledMessage::NAME.to_string(),
            hash: UnhandledMessage::HASH,
            payload: serde_json::to_value(&unhandled_message)?,
            attempted: 0,
        };

        // Try to handle the message - should return RegistryError
        let result = registry.handle(raw_message).await;

        match result {
            Err(registry_error) => {
                assert_eq!(registry_error.hash, UnhandledMessage::HASH);
                assert_eq!(registry_error.name, UnhandledMessage::NAME);
            }
            Ok(_) => panic!("Expected RegistryError for unhandled message"),
        }

        Ok(())
    }
}
