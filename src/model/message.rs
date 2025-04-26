use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: Uuid,
    pub key: String,
    pub value: Vec<u8>,
    pub topic: String,
    pub timestamp: DateTime<Utc>,
    pub headers: MessageHeaders,
    pub version: Option<u64>,
    pub expected_version: Option<u64>, // 낙관적 락을 위한 필드
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MessageHeaders {
    pub content_type: Option<String>,
    pub correlation_id: Option<String>,
    pub custom_headers: std::collections::HashMap<String, String>,
}

impl Message {
    pub fn new(key: String, value: Vec<u8>, topic: String) -> Self {
        Self {
            id: Uuid::new_v4(),
            key,
            value,
            topic,
            timestamp: Utc::now(),
            headers: MessageHeaders::default(),
            version: None,
            expected_version: None,
        }
    }

    pub fn with_expected_version(mut self, version: u64) -> Self {
        self.expected_version = Some(version);
        self
    }
}

// 스냅샷 정의
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub key: String,
    pub topic: String,
    pub value: Vec<u8>,
    pub version: u64,
    pub timestamp: DateTime<Utc>,
}
