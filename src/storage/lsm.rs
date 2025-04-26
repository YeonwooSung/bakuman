use crate::storage::{Storage, StorageExt};
// Bytes 가져오기 제거 (사용하지 않음)
use serde::{de::DeserializeOwned, Serialize};
use std::collections::BTreeMap;
use std::error::Error;
use std::sync::{Arc, RwLock};
use tokio::sync::RwLock as AsyncRwLock;

pub struct LSMTree<S: Storage> {
    // 메모리 테이블 (MemTable)
    memtable: Arc<RwLock<BTreeMap<String, Vec<u8>>>>,
    
    // 불변 메모리 테이블들 (Immutable MemTables)
    immutable_memtables: Arc<AsyncRwLock<Vec<Arc<BTreeMap<String, Vec<u8>>>>>>,
    
    // 영구 스토리지 백엔드
    storage: Arc<S>,
    
    // 플러시 임계값
    flush_threshold: usize,
}

impl<S: Storage + 'static> LSMTree<S> {
    pub fn new(storage: Arc<S>, flush_threshold: usize) -> Self {
        Self {
            memtable: Arc::new(RwLock::new(BTreeMap::new())),
            immutable_memtables: Arc::new(AsyncRwLock::new(Vec::new())),
            storage,
            flush_threshold,
        }
    }
    
    pub async fn put<T: Serialize + Send + Sync + 'static>(&self, key: &str, value: &T) -> Result<(), Box<dyn Error + Send + Sync>> {
        let serialized = serde_json::to_vec(value)?;
        
        // 메모리 테이블에 추가
        {
            let mut memtable = self.memtable.write().unwrap();
            memtable.insert(key.to_string(), serialized);
            
            // 메모리 테이블 크기 확인
            if memtable.len() >= self.flush_threshold {
                // 플러시 필요
                let old_memtable = std::mem::replace(&mut *memtable, BTreeMap::new());
                let old_memtable_arc = Arc::new(old_memtable);
                
                // 불변 메모리 테이블에 추가
                let mut immutable_tables = self.immutable_memtables.write().await;
                immutable_tables.push(old_memtable_arc.clone());
                
                // 백그라운드에서 디스크에 플러시 실행
                let storage = self.storage.clone();
                let old_memtable_clone = old_memtable_arc.clone();
                
                tokio::spawn(async move {
                    Self::flush_memtable_to_disk(storage, old_memtable_clone).await.unwrap();
                });
            }
        }
        
        Ok(())
    }
    
    pub async fn get<T: DeserializeOwned + Send + Sync + 'static>(&self, key: &str) -> Result<Option<T>, Box<dyn Error + Send + Sync>> {
        // 1. 먼저 메모리 테이블 확인
        {
            let memtable = self.memtable.read().unwrap();
            if let Some(value) = memtable.get(key) {
                return Ok(Some(serde_json::from_slice(value)?));
            }
        }
        
        // 2. 불변 메모리 테이블들 확인 (최신 순으로)
        {
            let immutable_tables = self.immutable_memtables.read().await;
            for table in immutable_tables.iter().rev() {
                if let Some(value) = table.get(key) {
                    return Ok(Some(serde_json::from_slice(value)?));
                }
            }
        }
        
        // 3. 디스크 스토리지 확인
        self.storage.get(key).await
    }
    
    async fn flush_memtable_to_disk(
        storage: Arc<S>, 
        memtable: Arc<BTreeMap<String, Vec<u8>>>
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 각 키-값 쌍을 디스크에 저장
        for (key, value) in memtable.iter() {
            // 직접 raw 바이트를 저장
            storage.store_raw(key, value).await?;
        }
        
        Ok(())
    }
}
