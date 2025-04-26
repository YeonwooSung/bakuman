use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::error::Error;

pub mod lsm;
pub mod object_store;

// 타입 이레이저(type erasure) 패턴 적용
// 객체 안전성 문제를 해결하기 위해 제네릭 메서드를 별도의 트레이트로 분리
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    async fn store_raw(&self, key: &str, value: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn get_raw(&self, key: &str) -> Result<Option<Vec<u8>>, Box<dyn Error + Send + Sync>>;
    async fn delete(&self, key: &str) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn list_keys_with_prefix(&self, prefix: &str) -> Result<Vec<String>, Box<dyn Error + Send + Sync>>;
}

// 확장 트레이트로 제네릭 편의 메서드 제공
#[async_trait]
pub trait StorageExt: Storage {
    async fn store<T: Serialize + Send + Sync>(&self, key: &str, value: &T) -> Result<(), Box<dyn Error + Send + Sync>> {
        let serialized = serde_json::to_vec(value)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
        self.store_raw(key, &serialized).await
    }

    async fn get<T: DeserializeOwned + Send + Sync>(&self, key: &str) -> Result<Option<T>, Box<dyn Error + Send + Sync>> {
        if let Some(data) = self.get_raw(key).await? {
            let value = serde_json::from_slice(&data)
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }
}

// 모든 Storage 구현체에 대해 StorageExt를 자동으로 구현
// ?Sized를 추가하여 dyn Storage와 같은 크기를 알 수 없는 타입에도 적용 가능하게 함
impl<T: ?Sized + Storage> StorageExt for T {}
