use chrono::{DateTime, Utc};
use fx_mq_building_blocks::queries::Queries;
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

/// Error returned when lease renewal fails.
#[derive(Debug, thiserror::Error)]
pub enum LeaseRenewalError {
    #[error("Database error during lease renewal")]
    Database(#[from] sqlx::Error),
}

/// Secure wrapper for renewing message leases during processing.
///
/// Provides handlers with the ability to extend their lease on a specific message
/// without exposing the full database API. Only allows renewal of the message
/// this renewer was created for.
///
/// # Examples
///
/// ```no_run
/// # use fx_mq_jobs::LeaseRenewer;
/// # use chrono::Utc;
/// # use sqlx::PgPool;
/// # async fn example(lease_renewer: LeaseRenewer, pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
/// let now = Utc::now();
///
/// match lease_renewer.renew_lease(now, &pool).await {
///     Ok(Some(expires_at)) => {
///         println!("Lease renewed until: {}", expires_at);
///     },
///     Ok(None) => {
///         println!("Could not renew - another host may have taken over");
///     },
///     Err(e) => {
///         println!("Renewal failed: {}", e);
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct LeaseRenewer {
    queries: Arc<Queries>,
    message_id: Uuid,
    host_id: Uuid,
    hold_for: Duration,
}

impl LeaseRenewer {
    /// Creates a new lease renewer for a specific message.
    ///
    /// This is typically created internally by the job processing system
    /// and passed to handlers.
    ///
    /// # Arguments
    ///
    /// * `queries` - Database query interface
    /// * `message_id` - ID of the message this renewer operates on
    /// * `host_id` - ID of the host that should own the lease
    /// * `hold_for` - Duration to extend the lease by
    #[tracing::instrument(
        skip(queries),
        fields(
            message_id = %message_id,
            host_id = %host_id,
            hold_for_secs = hold_for.as_secs()
        ),
        level = "debug"
    )]
    pub(crate) fn new(
        queries: Arc<Queries>,
        message_id: Uuid,
        host_id: Uuid,
        hold_for: Duration,
    ) -> Self {
        Self {
            queries,
            message_id,
            host_id,
            hold_for,
        }
    }

    /// Renews the lease on the associated message.
    ///
    /// Attempts to extend the lease duration from the current timestamp.
    /// Only succeeds if this host currently holds the lease or no other
    /// host has an active lease.
    ///
    /// # Arguments
    ///
    /// * `now` - Current timestamp for lease calculations
    /// * `pool` - Database connection pool
    ///
    /// # Returns
    ///
    /// * `Ok(Some(expires_at))` - Lease renewed successfully
    /// * `Ok(None)` - Could not renew lease (another host may have taken over)
    /// * `Err(LeaseRenewalError)` - Database error occurred
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use fx_mq_jobs::LeaseRenewer;
    /// # use chrono::Utc;
    /// # use sqlx::PgPool;
    /// # async fn example(lease_renewer: LeaseRenewer, pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    /// if let Some(expires_at) = lease_renewer.renew_lease(Utc::now(), &pool).await? {
    ///     println!("Lease extended until: {}", expires_at);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[tracing::instrument(
        skip(self, pool),
        fields(
            message_id = %self.message_id,
            host_id = %self.host_id,
            hold_for_secs = self.hold_for.as_secs()
        ),
        level = "debug",
        ret,
        err
    )]
    pub async fn renew_lease(
        &self,
        now: DateTime<Utc>,
        pool: &PgPool,
    ) -> Result<Option<DateTime<Utc>>, LeaseRenewalError> {
        // Start a new transaction for the lease renewal operation
        let mut tx = pool.begin().await?;

        // Attempt to renew the lease using the stored parameters
        let result = self
            .queries
            .request_lease(&mut tx, self.message_id, now, self.host_id, self.hold_for)
            .await?;

        // Commit the transaction to make the lease renewal permanent
        tx.commit().await?;

        // Log renewal result for important lease events
        match result {
            Some(expires_at) => {
                tracing::debug!(
                    message_id = %self.message_id,
                    expires_at = %expires_at,
                    "Lease successfully renewed"
                );
            }
            None => {
                tracing::warn!(
                    message_id = %self.message_id,
                    host_id = %self.host_id,
                    "Failed to renew lease - another host may have taken over"
                );
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use chrono::SubsecRound;
    use fx_mq_building_blocks::{migrator::run_migrations, testing_tools::TestMessage};

    use crate::{FX_MQ_JOBS_SCHEMA_NAME, Publisher};

    use super::*;

    #[sqlx::test(migrations = false)]
    async fn it_renews_the_lease(pool: sqlx::PgPool) -> anyhow::Result<()> {
        run_migrations(&pool, FX_MQ_JOBS_SCHEMA_NAME).await?;

        let host_id = Uuid::now_v7();

        let hold_for = Duration::from_mins(1);

        let queries = Arc::new(Queries::new(FX_MQ_JOBS_SCHEMA_NAME));

        let publisher = Publisher::<PgPool>::new(&pool, &queries);

        let published = publisher.publish(&TestMessage::default()).await?;

        let lease_renewer = LeaseRenewer::new(queries, published.id, host_id, hold_for);

        let now = Utc::now();
        let leased_until = lease_renewer
            .renew_lease(now, &pool)
            .await?
            .expect("Expected a lease to be acquired");
        assert_eq!(leased_until, (now + hold_for).trunc_subsecs(6));

        tokio::time::sleep(Duration::from_millis(1)).await;

        let now = Utc::now();
        let leased_until = lease_renewer
            .renew_lease(now, &pool)
            .await?
            .expect("Expected a lease to be acquired");
        assert_eq!(leased_until, (now + hold_for).trunc_subsecs(6));

        Ok(())
    }
}
