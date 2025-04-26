use crate::{
    model::message::Message,
    plugins::trait_def::{AuthAction, AuthorizationPlugin, MessageFilter, MessageTransformer},
};
use async_trait::async_trait;
use std::{collections::HashMap, error::Error, sync::RwLock};

// 메시지 변환 플러그인 예제 (헤더 추가)
pub struct HeaderEnrichmentTransformer {
    headers: HashMap<String, String>,
}

impl HeaderEnrichmentTransformer {
    pub fn new(headers: HashMap<String, String>) -> Self {
        Self { headers }
    }
}

#[async_trait]
impl MessageTransformer for HeaderEnrichmentTransformer {
    async fn transform(&self, mut message: Message) -> Result<Message, Box<dyn Error + Send + Sync>> {
        // 헤더 추가
        for (key, value) in &self.headers {
            message.headers.custom_headers.insert(key.clone(), value.clone());
        }
        
        Ok(message)
    }
}

// 메시지 필터 플러그인 예제 (속도 제한)
pub struct RateLimiterFilter {
    // 간단한 구현을 위한 인메모리 카운터
    // 실제로는 Redis 등을 사용할 수 있음
    counters: RwLock<HashMap<String, (std::time::Instant, u32)>>,
    max_rate: u32,
    time_window_secs: u64,
}

impl RateLimiterFilter {
    pub fn new(max_rate: u32, time_window_secs: u64) -> Self {
        Self {
            counters: RwLock::new(HashMap::new()),
            max_rate,
            time_window_secs,
        }
    }
}

#[async_trait]
impl MessageFilter for RateLimiterFilter {
    async fn filter(&self, message: &Message) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let now = std::time::Instant::now();
        let key = format!("{}:{}", message.topic, message.key);
        
        let mut counters = self.counters.write().unwrap();
        let entry = counters.entry(key.clone()).or_insert((now, 0));
        
        // 시간 창이 지났으면 카운터 리셋
        if now.duration_since(entry.0).as_secs() > self.time_window_secs {
            *entry = (now, 1);
            return Ok(true);
        }
        
        // 속도 제한 확인
        if entry.1 >= self.max_rate {
            return Ok(false); // 속도 제한 초과
        }
        
        // 카운터 증가
        entry.1 += 1;
        Ok(true)
    }
}

// 인증 플러그인 예제
pub struct SimpleAuthPlugin {
    permissions: HashMap<String, Vec<(String, Vec<AuthAction>)>>,
}

impl SimpleAuthPlugin {
    pub fn new() -> Self {
        Self {
            permissions: HashMap::new(),
        }
    }
    
    pub fn add_permission(&mut self, client_id: &str, topic: &str, actions: Vec<AuthAction>) {
        let client_perms = self.permissions.entry(client_id.to_string()).or_insert_with(Vec::new);
        client_perms.push((topic.to_string(), actions));
    }
}

#[async_trait]
impl AuthorizationPlugin for SimpleAuthPlugin {
    async fn authorize(&self, client_id: &str, topic: &str, action: &AuthAction) -> Result<bool, Box<dyn Error + Send + Sync>> {
        if let Some(permissions) = self.permissions.get(client_id) {
            for (topic_pattern, actions) in permissions {
                if topic.starts_with(topic_pattern) {
                    // 간단한 구현을 위해 시작 부분만 확인
                    // 실제로는 더 복잡한 패턴 매칭이 필요할 수 있음
                    
                    // 요청된 작업이 허용된 작업 목록에 있는지 확인
                    let action_allowed = actions.iter().any(|a| {
                        match (a, action) {
                            (AuthAction::Produce, AuthAction::Produce) => true,
                            (AuthAction::Consume, AuthAction::Consume) => true,
                            (AuthAction::CreateTopic, AuthAction::CreateTopic) => true,
                            (AuthAction::DeleteTopic, AuthAction::DeleteTopic) => true,
                            _ => false,
                        }
                    });
                    
                    if action_allowed {
                        return Ok(true);
                    }
                }
            }
        }
        
        Ok(false)
    }
}
