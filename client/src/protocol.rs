use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Message;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Publish {
        message: Message,
    },
    Subscribe {
        topic_pattern: String,
    },
    Unsubscribe {
        subscription_id: uuid::Uuid,
    },
    ConsumeByKey {
        topic: String,
        key: String,
        start_version: Option<u64>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Success(Value),
    Error(ErrorResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ErrorResponse {
    ConnectionError(String),
    AuthenticationError(String),
    Timeout(String),
    ProtocolError(String),
    ServerError(String),
    ValidationError(String),
    OptimisticLockError { expected: u64, actual: u64 },
    UnknownError(String),
}
