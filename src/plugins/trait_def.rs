use async_trait::async_trait;
use crate::model::message::Message;
use std::error::Error;

#[async_trait]
pub trait MessageTransformer: Send + Sync {
    async fn transform(&self, message: Message) -> Result<Message, Box<dyn Error + Send + Sync>>;
}

#[async_trait]
pub trait MessageFilter: Send + Sync {
    async fn filter(&self, message: &Message) -> Result<bool, Box<dyn Error + Send + Sync>>;
}

#[async_trait]
pub trait AuthorizationPlugin: Send + Sync {
    async fn authorize(&self, client_id: &str, topic: &str, action: &AuthAction) -> Result<bool, Box<dyn Error + Send + Sync>>;
}

#[derive(Debug, Clone)]
pub enum AuthAction {
    Produce,
    Consume,
    CreateTopic,
    DeleteTopic,
}

// 플러그인 매니저
pub struct PluginManager {
    transformers: Vec<Box<dyn MessageTransformer>>,
    filters: Vec<Box<dyn MessageFilter>>,
    auth_plugins: Vec<Box<dyn AuthorizationPlugin>>,
}

impl PluginManager {
    pub fn new() -> Self {
        Self {
            transformers: Vec::new(),
            filters: Vec::new(),
            auth_plugins: Vec::new(),
        }
    }
    
    pub fn register_transformer(&mut self, transformer: Box<dyn MessageTransformer>) {
        self.transformers.push(transformer);
    }
    
    pub fn register_filter(&mut self, filter: Box<dyn MessageFilter>) {
        self.filters.push(filter);
    }
    
    pub fn register_auth_plugin(&mut self, plugin: Box<dyn AuthorizationPlugin>) {
        self.auth_plugins.push(plugin);
    }
    
    pub async fn apply_transformers(&self, message: Message) -> Result<Message, Box<dyn Error + Send + Sync>> {
        let mut result = message;
        
        for transformer in &self.transformers {
            result = transformer.transform(result).await?;
        }
        
        Ok(result)
    }
    
    pub async fn apply_filters(&self, message: &Message) -> Result<bool, Box<dyn Error + Send + Sync>> {
        for filter in &self.filters {
            if !filter.filter(message).await? {
                return Ok(false);
            }
        }
        
        Ok(true)
    }
    
    pub async fn authorize(&self, client_id: &str, topic: &str, action: &AuthAction) -> Result<bool, Box<dyn Error + Send + Sync>> {
        for plugin in &self.auth_plugins {
            if !plugin.authorize(client_id, topic, action).await? {
                return Ok(false);
            }
        }
        
        Ok(true)
    }
}
