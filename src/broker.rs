use crate::{
    concurrency::optimistic_lock::OptimisticLockManager,
    model::{
        config::BrokerConfig,
        message::{Message, Snapshot},
        schema::SchemaRegistry,
        topic::{Subscription, TopicPath},
    },
    plugins::trait_def::{AuthAction, PluginManager},
    storage::{object_store::S3Storage, Storage, StorageExt},
};
// 사용하지 않는 imports 제거
use std::{collections::HashMap, error::Error, sync::Arc};
use thiserror::Error;
use tokio::{
    net::TcpListener,
    sync::RwLock,
};
use tracing::{error, info};
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("Storage error: {0}")]
    StorageError(String),
    
    #[error("Schema validation error: {0}")]
    SchemaError(String),
    
    #[error("Optimistic lock error: {0}")]
    OptimisticLockError(String),
    
    #[error("Topic not found: {0}")]
    TopicNotFound(String),
    
    #[error("Client not authorized: {0}")]
    NotAuthorized(String),
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

type ClientId = String;
type SubscriptionId = Uuid;

pub struct Broker {
    config: BrokerConfig,
    storage: Arc<dyn Storage>,
    lock_manager: Arc<OptimisticLockManager>,
    schema_registry: Arc<RwLock<SchemaRegistry>>,
    plugin_manager: Arc<RwLock<PluginManager>>,
    subscriptions: Arc<RwLock<HashMap<ClientId, HashMap<SubscriptionId, Subscription>>>>,
}

impl Broker {
    pub async fn new(config: BrokerConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // 스토리지 초기화
        let storage: Arc<dyn Storage> = match config.storage_type.as_str() {
            "s3" => {
                let s3_storage = S3Storage::new(&config.s3_region, &config.s3_bucket).await?;
                Arc::new(s3_storage)
            }
            _ => return Err("Unsupported storage type".into()),
        };

        Ok(Self {
            config,
            storage,
            lock_manager: Arc::new(OptimisticLockManager::new()),
            schema_registry: Arc::new(RwLock::new(SchemaRegistry::new())),
            plugin_manager: Arc::new(RwLock::new(PluginManager::new())),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    // 브로커 실행
    pub async fn run(&self) -> Result<(), Box<dyn Error>> {
        let addr = format!("0.0.0.0:{}", self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        info!("Broker listening on {}", addr);

        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    info!("New client connected: {}", addr);
                    let broker = self.clone_arc();
                    tokio::spawn(async move {
                        if let Err(err) = Self::handle_client(broker, socket).await {
                            error!("Error handling client {}: {}", addr, err);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }

    // 클라이언트 핸들링 (실제 구현은 더 복잡할 것임)
    async fn handle_client(
        _broker: Arc<Self>,
        _socket: tokio::net::TcpStream,
    ) -> Result<(), Box<dyn Error>> {
        // 요청/응답 프로토콜 구현 필요
        // 여기서는 예시로 생략
        Ok(())
    }

    // 메시지 발행
    pub async fn publish(&self, message: &mut Message, client_id: &str) -> Result<(), BrokerError> {
        // 권한 확인
        let auth_action = AuthAction::Produce;
        if !self
            .plugin_manager
            .read()
            .await
            .authorize(client_id, &message.topic, &auth_action)
            .await
            .map_err(|e| BrokerError::NotAuthorized(e.to_string()))?
        {
            return Err(BrokerError::NotAuthorized(format!(
                "Client {} not authorized to publish to topic {}",
                client_id, message.topic
            )));
        }

        // 스키마 검증
        // 여기서는 생략

        // 낙관적 락 처리
        self.lock_manager
            .validate_and_update(message)
            .map_err(|e| BrokerError::OptimisticLockError(e.to_string()))?;

        // 플러그인 필터 적용
        if !self
            .plugin_manager
            .read()
            .await
            .apply_filters(message)
            .await
            .map_err(|e| BrokerError::NotAuthorized(e.to_string()))?
        {
            return Err(BrokerError::NotAuthorized(
                "Message rejected by filter".to_string(),
            ));
        }

        // 메시지 변환 적용
        let transformed_message = self
            .plugin_manager
            .read()
            .await
            .apply_transformers(message.clone())
            .await
            .map_err(|e| BrokerError::StorageError(e.to_string()))?;

        // 스토리지에 저장
        let key = format!(
            "topics/{}/keys/{}/messages/{}",
            transformed_message.topic, transformed_message.key, transformed_message.id
        );
        self.storage
            .store(&key, &transformed_message)
            .await
            .map_err(|e| BrokerError::StorageError(e.to_string()))?;

        // 스냅샷 확인 및 생성
        self.check_and_create_snapshot(&transformed_message).await?;

        // 구독자에게 메시지 전달 (예시에서는 생략)

        Ok(())
    }

    // 스냅샷 체크 및 생성
    async fn check_and_create_snapshot(&self, message: &Message) -> Result<(), BrokerError> {
        // 버전 확인
        if let Some(version) = message.version {
            if version % self.config.snapshot_interval == 0 {
                // 스냅샷 생성
                let snapshot = Snapshot {
                    key: message.key.clone(),
                    topic: message.topic.clone(),
                    value: message.value.clone(),
                    version,
                    timestamp: message.timestamp,
                };

                // 스냅샷 저장
                let key = format!(
                    "topics/{}/keys/{}/snapshots/{}",
                    snapshot.topic, snapshot.key, snapshot.version
                );
                self.storage
                    .store(&key, &snapshot)
                    .await
                    .map_err(|e| BrokerError::StorageError(e.to_string()))?;

                info!(
                    "Created snapshot for key {} in topic {} at version {}",
                    snapshot.key, snapshot.topic, snapshot.version
                );
            }
        }

        Ok(())
    }

    // 구독 등록
    pub async fn subscribe(
        &self,
        client_id: &str,
        topic_pattern: &str,
    ) -> Result<SubscriptionId, BrokerError> {
        // 권한 확인
        let auth_action = AuthAction::Consume;
        if !self
            .plugin_manager
            .read()
            .await
            .authorize(client_id, topic_pattern, &auth_action)
            .await
            .map_err(|e| BrokerError::NotAuthorized(e.to_string()))?
        {
            return Err(BrokerError::NotAuthorized(format!(
                "Client {} not authorized to subscribe to topic pattern {}",
                client_id, topic_pattern
            )));
        }

        let subscription_id = Uuid::new_v4();
        let topic_path = TopicPath::new(topic_pattern);

        let mut subscriptions = self.subscriptions.write().await;
        let client_subs = subscriptions.entry(client_id.to_string()).or_insert_with(HashMap::new);
        
        client_subs.insert(
            subscription_id,
            Subscription {
                client_id: client_id.to_string(),
                topic_patterns: vec![topic_path].into_iter().collect(),
            },
        );

        info!("Client {} subscribed to {} with ID {}", client_id, topic_pattern, subscription_id);
        
        Ok(subscription_id)
    }

    // 구독 취소
    pub async fn unsubscribe(&self, client_id: &str, subscription_id: SubscriptionId) -> Result<(), BrokerError> {
        let mut subscriptions = self.subscriptions.write().await;
        
        if let Some(client_subs) = subscriptions.get_mut(client_id) {
            if client_subs.remove(&subscription_id).is_some() {
                info!("Client {} unsubscribed from ID {}", client_id, subscription_id);
                return Ok(());
            }
        }
        
        Err(BrokerError::NotAuthorized(format!(
            "Subscription ID {} not found for client {}",
            subscription_id, client_id
        )))
    }

    // 키 기반 메시지 소비
    pub async fn consume_by_key(
        &self,
        client_id: &str,
        topic: &str,
        key: &str,
        start_version: Option<u64>,
    ) -> Result<Vec<Message>, BrokerError> {
        // 권한 확인
        let auth_action = AuthAction::Consume;
        if !self
            .plugin_manager
            .read()
            .await
            .authorize(client_id, topic, &auth_action)
            .await
            .map_err(|e| BrokerError::NotAuthorized(e.to_string()))?
        {
            return Err(BrokerError::NotAuthorized(format!(
                "Client {} not authorized to consume from topic {}",
                client_id, topic
            )));
        }

        // 먼저 최신 스냅샷 찾기
        let mut messages = Vec::new();
        let mut current_version = start_version.unwrap_or(0);
        
        // 스냅샷 찾기 (있는 경우)
        if start_version.is_none() {
            let snapshot_prefix = format!("topics/{}/keys/{}/snapshots/", topic, key);
            let snapshot_keys = self
                .storage
                .list_keys_with_prefix(&snapshot_prefix)
                .await
                .map_err(|e| BrokerError::StorageError(e.to_string()))?;
                
            if !snapshot_keys.is_empty() {
                // 버전 번호로 정렬 (내림차순)
                let mut sorted_keys = snapshot_keys;
                sorted_keys.sort_by(|a, b| b.cmp(a)); // 최신 스냅샷이 먼저 오도록
                
                // 최신 스냅샷 가져오기
                if let Some(latest_snapshot_key) = sorted_keys.first() {
                    if let Some(snapshot) = self
                        .storage
                        .get::<Snapshot>(latest_snapshot_key)
                        .await
                        .map_err(|e| BrokerError::StorageError(e.to_string()))?
                    {
                        // 스냅샷 버전부터 시작
                        current_version = snapshot.version;
                        
                        // 스냅샷을 메시지로 변환하여 결과에 추가
                        let snapshot_message = Message {
                            id: Uuid::new_v4(),
                            key: snapshot.key.clone(),
                            value: snapshot.value.clone(),
                            topic: snapshot.topic.clone(),
                            timestamp: snapshot.timestamp,
                            headers: Default::default(),
                            version: Some(snapshot.version),
                            expected_version: None,
                        };
                        
                        messages.push(snapshot_message);
                    }
                }
            }
        }
        
        // 스냅샷 이후 메시지 가져오기
        let messages_prefix = format!("topics/{}/keys/{}/messages/", topic, key);
        let message_keys = self
            .storage
            .list_keys_with_prefix(&messages_prefix)
            .await
            .map_err(|e| BrokerError::StorageError(e.to_string()))?;
            
        for message_key in message_keys {
            if let Some(message) = self
                .storage
                .get::<Message>(&message_key)
                .await
                .map_err(|e| BrokerError::StorageError(e.to_string()))?
            {
                if let Some(version) = message.version {
                    if version > current_version {
                        messages.push(message);
                    }
                }
            }
        }
        
        // 버전 순서로 정렬
        messages.sort_by(|a, b| {
            let ver_a = a.version.unwrap_or(0);
            let ver_b = b.version.unwrap_or(0);
            ver_a.cmp(&ver_b)
        });
        
        Ok(messages)
    }

    // 브로커 복제본 생성 (클론)
    fn clone_arc(&self) -> Arc<Self> {
        Arc::new(Self {
            config: self.config.clone(),
            storage: self.storage.clone(),
            lock_manager: self.lock_manager.clone(),
            schema_registry: self.schema_registry.clone(),
            plugin_manager: self.plugin_manager.clone(),
            subscriptions: self.subscriptions.clone(),
        })
    }

    // 스키마 등록
    pub async fn register_schema(&self, topic: &str, schema_json: serde_json::Value) -> Result<(), BrokerError> {
        let mut registry = self.schema_registry.write().await;
        registry
            .register_schema(topic, schema_json)
            .map_err(|e| BrokerError::SchemaError(e.to_string()))
    }
}
