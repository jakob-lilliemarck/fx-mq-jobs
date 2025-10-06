use crate::handler::Registry;
use crate::listener::worker::WorkerHandle;
use crate::{LeaseRenewer, Publisher, RegistryBuilder};
use crate::{constants::FX_MQ_JOBS_SCHEMA_NAME, handler::Handler};
use futures::future::BoxFuture;
use fx_mq_building_blocks::backoff::ConstantBackoff;
use fx_mq_building_blocks::{
    models::{Message, RawMessage},
    queries::Queries,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

// A type to be able to identify handler invocations
pub(crate) type SharedState = Arc<Mutex<Vec<String>>>;

pub(crate) fn new_shared_state() -> SharedState {
    Arc::new(Mutex::new(Vec::new()))
}

#[derive(Debug, thiserror::Error)]
#[error("TestError: {message}")]
pub(crate) struct TestError {
    message: String,
}

pub(crate) fn format_log_message(handler_name: &str, called: u32) -> String {
    format!("{}:{}", handler_name, called)
}

// ============================================================
// Test utility handler that succeed
// ============================================================
#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct SucceedingHandlerMessage {
    message: String,
}

impl Default for SucceedingHandlerMessage {
    fn default() -> Self {
        Self {
            message: "success".to_string(),
        }
    }
}

impl SucceedingHandlerMessage {
    pub(crate) fn to_raw(&self) -> anyhow::Result<RawMessage> {
        let payload = serde_json::to_value(&self)?;

        Ok(RawMessage {
            id: Uuid::now_v7(),
            name: SucceedingHandlerMessage::NAME.to_string(),
            hash: SucceedingHandlerMessage::HASH,
            payload: payload,
            attempted: 0,
        })
    }
}

impl Message for SucceedingHandlerMessage {
    const NAME: &str = "SucceedingHandlerMessage";
}

pub(crate) struct SucceedingHandler {
    pub(crate) state: SharedState,
    pub(crate) backoff: ConstantBackoff,
    pub(crate) max_attempts: i32,
    pub(crate) called: Arc<Mutex<u32>>,
}

impl SucceedingHandler {
    pub(crate) fn new(state: SharedState, backoff: ConstantBackoff, max_attempts: i32) -> Self {
        Self {
            state,
            backoff,
            max_attempts,
            called: Arc::new(Mutex::new(0)),
        }
    }
}

impl Handler for SucceedingHandler {
    type Message = SucceedingHandlerMessage;
    type Error = TestError;

    fn handle<'a>(
        &'a self,
        _: Self::Message,
        _lease_renewer: LeaseRenewer,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        let state = self.state.clone();
        let called = self.called.clone();

        Box::pin(async move {
            let mut called_guard = called.lock().await;
            (*called_guard) += 1;
            let msg = format_log_message(Self::Message::NAME, *called_guard);

            let mut state_guard = state.lock().await;
            (*state_guard).push(msg);
            Ok(())
        })
    }

    fn try_at(
        &self,
        attempted: i32,
        attempted_at: chrono::DateTime<chrono::Utc>,
    ) -> chrono::DateTime<chrono::Utc> {
        self.backoff.try_at(attempted, attempted_at)
    }

    fn max_attempts(&self) -> i32 {
        self.max_attempts
    }
}

// ============================================================
// Test utiltities for handlers that error
// ============================================================
#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct FailingHandlerMessage {
    message: String,
}

impl FailingHandlerMessage {
    pub(crate) fn to_raw(&self) -> anyhow::Result<RawMessage> {
        let payload = serde_json::to_value(&self)?;

        Ok(RawMessage {
            id: Uuid::now_v7(),
            name: FailingHandlerMessage::NAME.to_string(),
            hash: FailingHandlerMessage::HASH,
            payload: payload,
            attempted: 0,
        })
    }
}

impl Message for FailingHandlerMessage {
    const NAME: &str = "FailingHandlerMessage";
}

impl Default for FailingHandlerMessage {
    fn default() -> Self {
        Self {
            message: "error".to_string(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct FailingHandler {
    pub(crate) state: SharedState,
    pub(crate) backoff: ConstantBackoff,
    pub(crate) max_attempts: i32,
    pub(crate) called: Arc<Mutex<u32>>,
}

impl FailingHandler {
    pub(crate) fn new(state: SharedState, backoff: ConstantBackoff, max_attempts: i32) -> Self {
        Self {
            state,
            max_attempts,
            backoff,
            called: Arc::new(Mutex::new(0)),
        }
    }
}

impl Handler for FailingHandler {
    type Message = FailingHandlerMessage;
    type Error = TestError;

    fn handle<'a>(
        &'a self,
        _: Self::Message,
        _lease_renewer: LeaseRenewer,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        let state = self.state.clone();
        let called = self.called.clone();

        Box::pin(async move {
            let mut called_guard = called.lock().await;
            (*called_guard) += 1;
            let msg = format_log_message(Self::Message::NAME, *called_guard);

            let mut guard = state.lock().await;
            (*guard).push(msg);
            Err(TestError {
                message: "some bullshit happened".to_string(),
            })
        })
    }

    fn try_at(
        &self,
        attempted: i32,
        attempted_at: chrono::DateTime<chrono::Utc>,
    ) -> chrono::DateTime<chrono::Utc> {
        self.backoff.try_at(attempted, attempted_at)
    }

    fn max_attempts(&self) -> i32 {
        self.max_attempts
    }
}

// ============================================================
// Test utiltities for a long running handler
// ============================================================
#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct LongRunningHandlerMessage {
    message: String,
}

impl Message for LongRunningHandlerMessage {
    const NAME: &str = "LongRunningHandlerMessage";
}

impl Default for LongRunningHandlerMessage {
    fn default() -> Self {
        Self {
            message: "long running".to_string(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct LongRunningHandler {
    pub(crate) state: SharedState,
    pub(crate) backoff: ConstantBackoff,
    pub(crate) max_attempts: i32,
    pub(crate) called: Arc<Mutex<u32>>,
}

impl LongRunningHandler {
    pub(crate) fn new(state: SharedState, backoff: ConstantBackoff, max_attempts: i32) -> Self {
        Self {
            state,
            max_attempts,
            backoff,
            called: Arc::new(Mutex::new(0)),
        }
    }
}

impl Handler for LongRunningHandler {
    type Message = LongRunningHandlerMessage;
    type Error = TestError;

    fn handle<'a>(
        &'a self,
        _: Self::Message,
        _lease_renewer: LeaseRenewer,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        let state = self.state.clone();
        let called = self.called.clone();

        Box::pin(async move {
            // Simulate a long running process with sleep
            tokio::time::sleep(Duration::from_mins(1)).await;

            let mut called_guard = called.lock().await;
            (*called_guard) += 1;
            let msg = format_log_message(Self::Message::NAME, *called_guard);

            let mut state_guard = state.lock().await;
            (*state_guard).push(msg);
            Ok(())
        })
    }

    fn try_at(
        &self,
        attempted: i32,
        attempted_at: chrono::DateTime<chrono::Utc>,
    ) -> chrono::DateTime<chrono::Utc> {
        self.backoff.try_at(attempted, attempted_at)
    }

    fn max_attempts(&self) -> i32 {
        self.max_attempts
    }
}

// ============================================================
// Test utilities for a handler that fails on the first attempt only
// ============================================================
#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct FailingOnFirstCallHandlerMessage {
    message: String,
}

impl Message for FailingOnFirstCallHandlerMessage {
    const NAME: &str = "FailingOnFirstCallHandlerMessage";
}

impl Default for FailingOnFirstCallHandlerMessage {
    fn default() -> Self {
        Self {
            message: "fails on first call".to_string(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct FailingOnFirstCallHandler {
    pub(crate) state: SharedState,
    pub(crate) backoff: ConstantBackoff,
    pub(crate) max_attempts: i32,
    pub(crate) called: Arc<Mutex<u32>>,
}

impl FailingOnFirstCallHandler {
    pub(crate) fn new(state: SharedState, backoff: ConstantBackoff, max_attempts: i32) -> Self {
        Self {
            state,
            max_attempts,
            backoff,
            called: Arc::new(Mutex::new(0)),
        }
    }
}

impl Handler for FailingOnFirstCallHandler {
    type Message = FailingOnFirstCallHandlerMessage;
    type Error = TestError;

    fn handle<'a>(
        &'a self,
        _: Self::Message,
        _lease_renewer: LeaseRenewer,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        let state = self.state.clone();
        let called = self.called.clone();

        Box::pin(async move {
            let mut called_guard = called.lock().await;
            (*called_guard) += 1;
            let msg = format_log_message(Self::Message::NAME, *called_guard);

            let mut state_guard = state.lock().await;
            (*state_guard).push(msg);

            if *called_guard == 1 {
                // Fail on first call
                Err(TestError {
                    message: "some bullshit happened".to_string(),
                })
            } else {
                // Succeed on subsequent calls
                Ok(())
            }
        })
    }

    fn try_at(
        &self,
        attempted: i32,
        attempted_at: chrono::DateTime<chrono::Utc>,
    ) -> chrono::DateTime<chrono::Utc> {
        self.backoff.try_at(attempted, attempted_at)
    }

    fn max_attempts(&self) -> i32 {
        self.max_attempts
    }
}

// ============================================================
// Test message with no handler registered
// ============================================================
#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct UnhandledMessage {
    message: String,
}

impl Message for UnhandledMessage {
    const NAME: &str = "UnhandledMessage";
}

impl Default for UnhandledMessage {
    fn default() -> Self {
        Self {
            message: "no handler for this".to_string(),
        }
    }
}

// ============================================================
// Utilities for creating test workers and registers
// ============================================================
pub(crate) fn new_test_registry(state: &SharedState) -> RegistryBuilder {
    let succeeding_handler = SucceedingHandler::new(
        state.clone(),
        ConstantBackoff::new(Duration::from_millis(5)),
        3,
    );

    let failing_handler = FailingHandler::new(
        state.clone(),
        ConstantBackoff::new(Duration::from_millis(5)),
        3,
    );

    let long_running_handler = LongRunningHandler::new(
        state.clone(),
        ConstantBackoff::new(Duration::from_millis(5)),
        3,
    );

    let failing_on_first_attempt_handler = FailingOnFirstCallHandler::new(
        state.clone(),
        ConstantBackoff::new(Duration::from_millis(6)),
        3,
    );

    RegistryBuilder::default()
        .with_handler(succeeding_handler)
        .with_handler(failing_handler)
        .with_handler(long_running_handler)
        .with_handler(failing_on_first_attempt_handler)
}

pub(crate) async fn new_worker_handle(
    pool: &PgPool,
    registry: &Arc<Registry>,
) -> anyhow::Result<(WorkerHandle, Receiver<usize>)> {
    let queries = Arc::new(Queries::new(FX_MQ_JOBS_SCHEMA_NAME));

    let (tx_idle, rx_idle) = mpsc::channel(1);

    let handle = WorkerHandle::new(0, &tx_idle, &registry, &queries, &pool).await?;

    Ok((handle, rx_idle))
}

pub(crate) fn new_pool_publisher(pool: &PgPool) -> Publisher<PgPool> {
    let queries = Arc::new(Queries::new(FX_MQ_JOBS_SCHEMA_NAME));
    Publisher::<PgPool>::new(pool, &queries)
}
