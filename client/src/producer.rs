use std::sync::Arc;

use anyhow::Result;
use serde::Serialize;

use crate::{Client, Message, MessageHeaders};

pub struct Producer<C: Client> {
    client: Arc<C>,
    default_topic: Option<String>,
}

impl<C: Client> Producer<C> {
    pub fn new(client: Arc<C>) -> Self {
        Self {
            client,
            default_topic: None,
        }
    }

    pub fn with_default_topic(mut self, topic: impl Into<String>) -> Self {
        self.default_topic = Some(topic.into());
        self
    }

    pub async fn publish<K, V>(&self, key: K, value: V, topic: Option<&str>) -> Result<Message>
    where
        K: Into<String>,
        V: Serialize,
    {
        let topic = topic
            .map(|t| t.to_string())
            .or_else(|| self.default_topic.clone())
            .ok_or_else(|| anyhow::anyhow!("No topic specified and no default topic set"))?;

        let serialized = serde_json::to_vec(&value)?;

        let message = Message::new(key, serialized, topic);

        self.client.publish(message).await
    }

    pub async fn publish_with_version<K, V>(
        &self,
        key: K,
        value: V,
        topic: Option<&str>,
        expected_version: u64,
    ) -> Result<Message>
    where
        K: Into<String>,
        V: Serialize,
    {
        let topic = topic
            .map(|t| t.to_string())
            .or_else(|| self.default_topic.clone())
            .ok_or_else(|| anyhow::anyhow!("No topic specified and no default topic set"))?;

        let serialized = serde_json::to_vec(&value)?;

        let message = Message::new(key, serialized, topic).with_expected_version(expected_version);

        self.client.publish(message).await
    }

    pub async fn publish_with_headers<K, V>(
        &self,
        key: K,
        value: V,
        topic: Option<&str>,
        headers: MessageHeaders,
    ) -> Result<Message>
    where
        K: Into<String>,
        V: Serialize,
    {
        let topic = topic
            .map(|t| t.to_string())
            .or_else(|| self.default_topic.clone())
            .ok_or_else(|| anyhow::anyhow!("No topic specified and no default topic set"))?;

        let serialized = serde_json::to_vec(&value)?;

        let message = Message::new(key, serialized, topic).with_headers(headers);

        self.client.publish(message).await
    }
}
