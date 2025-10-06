use std::sync::Arc;

use fx_mq_building_blocks::models::{Message, RawMessage};
use fx_mq_building_blocks::queries::Queries;
use sqlx::{PgPool, PgTransaction};
use uuid::Uuid;

#[derive(thiserror::Error, Debug)]
pub enum PublishError {
    #[error("Failed to serialize the message payload")]
    SerializationError {
        hash: i32,
        name: String,
        #[source]
        source: serde_json::Error,
    },

    #[error("Failed to write the message to the database")]
    DatabaseError(#[from] sqlx::Error),
}

/// Service for publishing typed messages to the job queue.
///
/// Supports both pool-based and transaction-scoped publishing patterns.
/// Messages are serialized to JSON and persisted with unique IDs.
///
/// # Examples
///
/// ```no_run
/// # use fx_mq_jobs::Publisher;
/// # use fx_mq_building_blocks::{models::Message, queries::Queries};
/// # use std::sync::Arc;
/// # use sqlx::{PgPool, PgTransaction};
/// # use serde::{Serialize, Deserialize};
/// # #[derive(Serialize, Deserialize, Clone)]
/// # struct MyMessage { data: String }
/// # impl Message for MyMessage {
/// #     const NAME: &'static str = "my_message";
/// #     const HASH: i32 = 12345;
/// # }
/// # async fn example(pool: PgPool, queries: Arc<Queries>) -> Result<(), Box<dyn std::error::Error>> {
/// let my_message = MyMessage { data: "hello".to_string() };
///
/// // Pool-based publishing
/// let publisher = Publisher::<PgPool>::new(&pool, &queries);
/// let raw_message = publisher.publish(&my_message).await?;
///
/// // Transaction-scoped publishing
/// let tx = pool.begin().await?;
/// let mut publisher = Publisher::<PgTransaction<'_>>::new(tx, &queries);
/// let raw_message = publisher.publish(&my_message).await?;
/// let tx: PgTransaction<'_> = publisher.into();
/// tx.commit().await?;
/// # Ok(())
/// # }
/// ```
pub struct Publisher<E> {
    executor: E,
    queries: Arc<Queries>,
}

impl Publisher<PgPool> {
    /// Creates a new pool-based publisher.
    ///
    /// # Arguments
    ///
    /// * `pool` - Database connection pool
    /// * `queries` - Pre-configured query builder for the target schema
    #[tracing::instrument(skip(pool, queries), level = "debug")]
    pub fn new(pool: &PgPool, queries: &Arc<Queries>) -> Self {
        Self {
            executor: pool.clone(),
            queries: queries.clone(),
        }
    }

    /// Publishes a typed message to the job queue using the pool.
    ///
    /// # Arguments
    ///
    /// * `message` - The typed message to publish
    ///
    /// # Returns
    ///
    /// The persisted `RawMessage` with generated ID and serialized payload.
    ///
    /// # Errors
    ///
    /// * `PublishError::SerializationError` - If message serialization fails
    /// * `PublishError::DatabaseError` - If database operations fail
    #[tracing::instrument(
        skip(self, message),
        fields(
            message_name = M::NAME,
            hash = M::HASH
        ),
        level = "info",
        err
    )]
    pub async fn publish<M: Message>(&self, message: &M) -> Result<RawMessage, PublishError> {
        let mut tx = self.executor.begin().await?;

        let payload =
            serde_json::to_value(message).map_err(|error| PublishError::SerializationError {
                hash: M::HASH,
                name: M::NAME.to_string(),
                source: error,
            })?;

        let raw_message = RawMessage {
            id: Uuid::now_v7(),
            name: M::NAME.to_string(),
            hash: M::HASH,
            payload,
            attempted: 0,
        };

        let raw_message = self.queries.publish_message(&mut tx, raw_message).await?;

        tx.commit().await?;
        Ok(raw_message)
    }
}

impl<'tx> Publisher<PgTransaction<'tx>> {
    /// Creates a new transaction-scoped publisher.
    ///
    /// The transaction must be manually committed after publishing.
    ///
    /// # Arguments
    ///
    /// * `tx` - Database transaction (ownership is transferred)
    /// * `queries` - Pre-configured query builder for the target schema
    #[tracing::instrument(skip(tx, queries), level = "debug")]
    pub fn new(tx: PgTransaction<'tx>, queries: &Arc<Queries>) -> Self {
        Self {
            executor: tx,
            queries: queries.clone(),
        }
    }

    /// Publishes a typed message within the current transaction.
    ///
    /// The transaction is not committed - use `publisher.into()` to extract
    /// the transaction and commit manually.
    ///
    /// # Arguments
    ///
    /// * `message` - The typed message to publish
    ///
    /// # Returns
    ///
    /// The persisted `RawMessage` with generated ID and serialized payload.
    /// Message will be visible to workers only after transaction commit.
    ///
    /// # Errors
    ///
    /// * `PublishError::SerializationError` - If message serialization fails
    /// * `PublishError::DatabaseError` - If database operations fail
    #[tracing::instrument(
        skip(self, message),
        fields(
            message_name = M::NAME,
            hash = M::HASH
        ),
        level = "info",
        err
    )]
    pub async fn publish<M: Message>(&mut self, message: &M) -> Result<RawMessage, PublishError> {
        let payload =
            serde_json::to_value(message).map_err(|error| PublishError::SerializationError {
                hash: M::HASH,
                name: M::NAME.to_string(),
                source: error,
            })?;

        let raw_message = RawMessage {
            id: Uuid::now_v7(),
            name: M::NAME.to_string(),
            hash: M::HASH,
            payload,
            attempted: 0,
        };

        let raw_message = self
            .queries
            .publish_message(&mut self.executor, raw_message)
            .await?;

        Ok(raw_message)
    }
}

impl<'tx> Into<PgTransaction<'tx>> for Publisher<PgTransaction<'tx>> {
    #[tracing::instrument(skip(self), level = "debug")]
    fn into(self) -> PgTransaction<'tx> {
        self.executor
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::constants::FX_MQ_JOBS_SCHEMA_NAME;
    use chrono::Utc;
    use fx_mq_building_blocks::{migrator::run_migrations, testing_tools::TestMessage};

    #[sqlx::test(migrations = false)]
    async fn it_publishes_using_a_pool(pool: sqlx::PgPool) -> anyhow::Result<()> {
        run_migrations(&pool, FX_MQ_JOBS_SCHEMA_NAME).await?;

        let queries = Arc::new(Queries::new(FX_MQ_JOBS_SCHEMA_NAME));

        let publisher = Publisher::<PgPool>::new(&pool, &queries);

        let message = TestMessage::new("hello".to_string(), 42);

        let published = publisher.publish(&message).await?;

        assert_eq!(published.name, TestMessage::NAME);
        assert_eq!(published.hash, TestMessage::HASH);
        assert_eq!(
            published.payload,
            serde_json::json!({"message": "hello", "value": 42})
        );

        let mut tx = pool.begin().await?;
        let is_pending = queries
            .is_pending(&mut tx, published.id, Utc::now())
            .await?;
        tx.commit().await?;

        assert!(is_pending);

        Ok(())
    }

    #[sqlx::test(migrations = false)]
    async fn it_publishes_using_a_transaction(pool: sqlx::PgPool) -> anyhow::Result<()> {
        run_migrations(&pool, FX_MQ_JOBS_SCHEMA_NAME).await?;

        let queries = Arc::new(Queries::new(FX_MQ_JOBS_SCHEMA_NAME));

        let tx = pool.begin().await?;

        let mut publisher = Publisher::<PgTransaction<'_>>::new(tx, &queries);

        let message = TestMessage::new("hello".to_string(), 42);

        let published = publisher.publish(&message).await?;

        let tx: PgTransaction<'_> = publisher.into();

        tx.commit().await?;

        assert_eq!(published.name, TestMessage::NAME);
        assert_eq!(published.hash, TestMessage::HASH);
        assert_eq!(
            published.payload,
            serde_json::json!({"message": "hello", "value": 42})
        );

        let mut tx = pool.begin().await?;
        let is_pending = queries
            .is_pending(&mut tx, published.id, Utc::now())
            .await?;
        tx.commit().await?;

        assert!(is_pending);

        Ok(())
    }
}
