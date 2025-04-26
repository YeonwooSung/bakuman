use crate::model::message::Message;
use dashmap::DashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum OptimisticLockError {
    #[error("Version conflict: expected {expected}, but found {actual}")]
    VersionConflict { expected: u64, actual: u64 },

    #[error("Key not found: {0}")]
    KeyNotFound(String),
}

#[derive(Debug)]
pub struct OptimisticLockManager {
    versions: DashMap<(String, String), u64>, // (topic, key) -> version
}

impl OptimisticLockManager {
    pub fn new() -> Self {
        Self {
            versions: DashMap::new(),
        }
    }

    pub fn validate_and_update(&self, message: &mut Message) -> Result<(), OptimisticLockError> {
        let key = (message.topic.clone(), message.key.clone());

        // 버전 검증
        if let Some(expected_version) = message.expected_version {
            let current_version = self.versions.get(&key).map(|v| *v).unwrap_or(0);

            if expected_version != current_version {
                return Err(OptimisticLockError::VersionConflict {
                    expected: expected_version,
                    actual: current_version,
                });
            }
        }

        // 새 버전 할당 및 업데이트
        let new_version = self.versions.get(&key).map(|v| *v + 1).unwrap_or(1);

        self.versions.insert(key, new_version);
        message.version = Some(new_version);

        Ok(())
    }
}
