use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;
use tracing::info;
use uuid::Uuid;

use crate::{Client, Message};

#[async_trait]
pub trait MessageHandler: Send + Sync {
    async fn handle(&self, message: Message) -> Result<()>;
}

pub struct Consumer<C: Client> {
    client: Arc<C>,
    subscriptions: Arc<RwLock<HashMap<String, ConsumerSubscription>>>, // Arc로 감싸기
    poll_interval: Duration,
}

struct ConsumerSubscription {
    topic_pattern: String,
    subscription_id: Uuid,
    handlers: Vec<Arc<dyn MessageHandler>>,
    polling_task: Option<JoinHandle<()>>,
    shutdown_tx: broadcast::Sender<()>,
}

impl<C: Client + 'static> Consumer<C> {
    pub fn new(client: Arc<C>) -> Self {
        Self {
            client,
            subscriptions: Arc::new(RwLock::new(HashMap::new())), // Arc로 초기화
            poll_interval: Duration::from_secs(1),
        }
    }

    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    pub async fn subscribe(
        &self,
        topic_pattern: &str,
        handler: Arc<dyn MessageHandler>,
    ) -> Result<()> {
        let subscription_id = self.client.subscribe(topic_pattern).await?;

        // Subscription 생성 또는 업데이트
        let mut subscriptions = self.subscriptions.write().await;

        if let Some(subscription) = subscriptions.get_mut(topic_pattern) {
            // 기존 구독에 핸들러 추가
            subscription.handlers.push(handler);
            Ok(())
        } else {
            // 새 구독 생성
            let (shutdown_tx, _) = broadcast::channel(1);

            let subscription = ConsumerSubscription {
                topic_pattern: topic_pattern.to_string(),
                subscription_id,
                handlers: vec![handler],
                polling_task: None,
                shutdown_tx: shutdown_tx.clone(),
            };

            // 구독 추가
            subscriptions.insert(topic_pattern.to_string(), subscription);

            // 폴링 태스크 시작
            self.start_polling_task(topic_pattern.to_string(), shutdown_tx)
                .await;

            Ok(())
        }
    }

    pub async fn unsubscribe(&self, topic_pattern: &str) -> Result<()> {
        let mut subscriptions = self.subscriptions.write().await;

        if let Some(subscription) = subscriptions.remove(topic_pattern) {
            // 서버 구독 취소
            self.client
                .unsubscribe(subscription.subscription_id)
                .await?;

            // 폴링 태스크 종료
            if let Some(task) = subscription.polling_task {
                let _ = subscription.shutdown_tx.send(());
                task.abort();
            }

            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "No subscription found for topic pattern: {}",
                topic_pattern
            ))
        }
    }

    async fn start_polling_task(&self, topic_pattern: String, shutdown_tx: broadcast::Sender<()>) {
        let client = self.client.clone();
        let poll_interval = self.poll_interval;
        let subscriptions = self.subscriptions.clone(); // 이제 Arc<RwLock<...>>를 복제 가능

        let task = tokio::spawn(async move {
            let mut shutdown_rx = shutdown_tx.subscribe();

            loop {
                // 종료 신호 확인
                if shutdown_rx.try_recv().is_ok() {
                    break;
                }

                // 메시지 폴링 및 처리 로직
                // 실제 구현에서는 topic_pattern에 맞는 모든 토픽에서 메시지를 가져와야 함
                // 여기서는 간단한 예제로 구성

                // 다음 폴링까지 대기
                tokio::time::sleep(poll_interval).await;
            }
        });

        // 태스크 핸들 저장
        let mut subscriptions_guard = self.subscriptions.write().await;
        if let Some(subscription) = subscriptions_guard.get_mut(&topic_pattern) {
            subscription.polling_task = Some(task);
        }
    }

    async fn process_message(&self, topic_pattern: &str, message: Message) -> Result<()> {
        let subscriptions = self.subscriptions.read().await;

        if let Some(subscription) = subscriptions.get(topic_pattern) {
            for handler in &subscription.handlers {
                handler.handle(message.clone()).await?;
            }
        }

        Ok(())
    }
}
