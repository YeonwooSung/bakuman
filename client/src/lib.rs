use anyhow::{Context, Result};
use async_trait::async_trait;
use backoff::future::retry;
use backoff::ExponentialBackoff;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

pub mod consumer;
pub mod producer;
pub mod protocol;

use protocol::{Command, ErrorResponse, Response};

// 클라이언트 에러 정의
#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Authentication error: {0}")]
    AuthenticationError(String),

    #[error("Timeout error: {0}")]
    TimeoutError(String),

    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Server error: {0}")]
    ServerError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

// 메시지 헤더 구조체
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MessageHeaders {
    pub content_type: Option<String>,
    pub correlation_id: Option<String>,
    pub custom_headers: std::collections::HashMap<String, String>,
}

// 메시지 구조체
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: Uuid,
    pub key: String,
    pub value: Vec<u8>,
    pub topic: String,
    pub timestamp: DateTime<Utc>,
    pub headers: MessageHeaders,
    pub version: Option<u64>,
    pub expected_version: Option<u64>,
}

impl Message {
    pub fn new(
        key: impl Into<String>,
        value: impl Into<Vec<u8>>,
        topic: impl Into<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            key: key.into(),
            value: value.into(),
            topic: topic.into(),
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

    pub fn with_headers(mut self, headers: MessageHeaders) -> Self {
        self.headers = headers;
        self
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.custom_headers.insert(key.into(), value.into());
        self
    }
}

// 클라이언트 설정
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub hosts: Vec<String>,
    pub client_id: String,
    pub connection_timeout: Duration,
    pub request_timeout: Duration,
    pub connection_retry_max_attempts: u32,
    pub connection_retry_base_delay: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            hosts: vec!["localhost:9092".to_string()],
            client_id: format!("bakuman-client-{}", Uuid::new_v4()),
            connection_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            connection_retry_max_attempts: 5,
            connection_retry_base_delay: Duration::from_millis(100),
        }
    }
}

// 클라이언트 인터페이스
#[async_trait]
pub trait Client: Send + Sync {
    async fn connect(&self) -> Result<()>;
    async fn close(&self) -> Result<()>;
    async fn publish(&self, message: Message) -> Result<Message>;
    async fn subscribe(&self, topic_pattern: &str) -> Result<Uuid>;
    async fn unsubscribe(&self, subscription_id: Uuid) -> Result<()>;
    async fn consume_by_key(
        &self,
        topic: &str,
        key: &str,
        start_version: Option<u64>,
    ) -> Result<Vec<Message>>;
}

// 클라이언트 구현
pub struct BakumanClient {
    config: ClientConfig,
    connection: Arc<RwLock<Option<ClientConnection>>>,
}

impl BakumanClient {
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config,
            connection: Arc::new(RwLock::new(None)),
        }
    }

    async fn ensure_connected(&self) -> Result<()> {
        let conn = self.connection.read().await;
        if conn.is_none() {
            drop(conn);
            self.connect().await?;
        }
        Ok(())
    }

    async fn reconnect_if_needed(&self) -> Result<()> {
        let mut conn = self.connection.write().await;
        if let Some(client_conn) = &*conn {
            if !client_conn.is_connected() {
                *conn = None;
                drop(conn);
                self.connect().await?;
            }
        } else {
            drop(conn);
            self.connect().await?;
        }
        Ok(())
    }

    async fn execute_command<T>(&self, command: Command) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.ensure_connected().await?;

        let retry_operation = || async {
            let result = {
                let conn_guard = self.connection.read().await;
                if let Some(conn) = &*conn_guard {
                    conn.send_command(command.clone()).await
                } else {
                    return Err(backoff::Error::permanent(anyhow::anyhow!(
                        "No connection available"
                    )));
                }
            };

            match result {
                Ok(response) => {
                    match response {
                        Response::Success(data) => {
                            let result: T = serde_json::from_value(data).map_err(|e| {
                                backoff::Error::permanent(anyhow::anyhow!(
                                    "Failed to deserialize response: {}",
                                    e
                                ))
                            })?;
                            Ok(result)
                        }
                        Response::Error(err) => {
                            match err {
                                ErrorResponse::ConnectionError(_) | ErrorResponse::Timeout(_) => {
                                    // 재연결이 필요한 오류
                                    self.reconnect_if_needed()
                                        .await
                                        .map_err(backoff::Error::permanent)?;
                                    Err(backoff::Error::transient(anyhow::anyhow!(
                                        "Connection error: {:?}",
                                        err
                                    )))
                                }
                                _ => {
                                    // 영구적인 오류
                                    Err(backoff::Error::permanent(anyhow::anyhow!(
                                        "Server error: {:?}",
                                        err
                                    )))
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    // 재연결이 필요한 오류
                    self.reconnect_if_needed()
                        .await
                        .map_err(backoff::Error::permanent)?;
                    Err(backoff::Error::transient(e))
                }
            }
        };

        let backoff = ExponentialBackoff {
            max_elapsed_time: Some(self.config.request_timeout),
            max_interval: Duration::from_secs(2),
            ..ExponentialBackoff::default()
        };

        retry(backoff, retry_operation).await
    }
}

#[async_trait]
impl Client for BakumanClient {
    async fn connect(&self) -> Result<()> {
        info!("Connecting to Bakuman servers: {:?}", self.config.hosts);

        let mut last_error = None;

        // 사용 가능한 호스트 중 하나에 연결 시도
        for host in &self.config.hosts {
            match self.connect_to_host(host).await {
                Ok(connection) => {
                    let mut conn_guard = self.connection.write().await;
                    *conn_guard = Some(connection);
                    info!("Successfully connected to {}", host);
                    return Ok(());
                }
                Err(e) => {
                    warn!("Failed to connect to {}: {}", host, e);
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("No hosts available to connect")))
    }

    async fn close(&self) -> Result<()> {
        let mut conn_guard = self.connection.write().await;
        if let Some(connection) = conn_guard.take() {
            connection.close().await?;
        }
        Ok(())
    }

    async fn publish(&self, message: Message) -> Result<Message> {
        debug!(
            "Publishing message to topic {}, key: {}",
            message.topic, message.key
        );

        let command = Command::Publish {
            message: message.clone(),
        };
        let response: Message = self.execute_command(command).await?;

        Ok(response)
    }

    async fn subscribe(&self, topic_pattern: &str) -> Result<Uuid> {
        debug!("Subscribing to topic pattern: {}", topic_pattern);

        let command = Command::Subscribe {
            topic_pattern: topic_pattern.to_string(),
        };

        let subscription_id: Uuid = self.execute_command(command).await?;

        debug!(
            "Subscribed to {} with ID: {}",
            topic_pattern, subscription_id
        );
        Ok(subscription_id)
    }

    async fn unsubscribe(&self, subscription_id: Uuid) -> Result<()> {
        debug!("Unsubscribing from subscription ID: {}", subscription_id);

        let command = Command::Unsubscribe { subscription_id };
        let _: () = self.execute_command(command).await?;

        debug!("Unsubscribed from ID: {}", subscription_id);
        Ok(())
    }

    async fn consume_by_key(
        &self,
        topic: &str,
        key: &str,
        start_version: Option<u64>,
    ) -> Result<Vec<Message>> {
        debug!(
            "Consuming messages for topic: {}, key: {}, start_version: {:?}",
            topic, key, start_version
        );

        let command = Command::ConsumeByKey {
            topic: topic.to_string(),
            key: key.to_string(),
            start_version,
        };

        let messages: Vec<Message> = self.execute_command(command).await?;

        debug!(
            "Received {} messages for key {} in topic {}",
            messages.len(),
            key,
            topic
        );

        Ok(messages)
    }
}

impl BakumanClient {
    async fn connect_to_host(&self, host: &str) -> Result<ClientConnection> {
        let connect_future = TcpStream::connect(host);
        let conn_timeout = self.config.connection_timeout;

        let stream = timeout(conn_timeout, connect_future)
            .await
            .context("Connection timeout")?
            .context("Failed to connect")?;

        stream
            .set_nodelay(true)
            .context("Failed to set TCP_NODELAY")?;

        let (reader, writer) = tokio::io::split(stream);

        let connection = ClientConnection::new(reader, writer, self.config.client_id.clone());

        // 초기 인증/핸드셰이크 수행
        connection.initialize().await?;

        Ok(connection)
    }
}

// 클라이언트 연결 관리
struct ClientConnection {
    reader: Arc<Mutex<tokio::io::ReadHalf<TcpStream>>>,
    writer: Arc<Mutex<tokio::io::WriteHalf<TcpStream>>>,
    client_id: String,
    connected: Arc<RwLock<bool>>,
    next_request_id: Arc<Mutex<u64>>,
    response_channels: Arc<Mutex<std::collections::HashMap<u64, mpsc::Sender<Result<Response>>>>>,
}

impl ClientConnection {
    fn new(
        reader: tokio::io::ReadHalf<TcpStream>,
        writer: tokio::io::WriteHalf<TcpStream>,
        client_id: String,
    ) -> Self {
        let connection = Self {
            reader: Arc::new(Mutex::new(reader)),
            writer: Arc::new(Mutex::new(writer)),
            client_id,
            connected: Arc::new(RwLock::new(false)),
            next_request_id: Arc::new(Mutex::new(0)),
            response_channels: Arc::new(Mutex::new(std::collections::HashMap::new())),
        };

        // 응답 수신 루프 시작
        tokio::spawn(connection.clone().response_loop());

        connection
    }

    async fn initialize(&self) -> Result<()> {
        // 실제 구현에서는 인증 및 핸드셰이크 프로토콜 수행
        // 여기서는 간단히 연결 상태만 설정
        *self.connected.write().await = true;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        // 현재 구현에서는 단순히 상태값만 반환
        // 실제 구현에서는 소켓 상태도 확인해야 함
        *self.connected.blocking_read()
    }

    async fn close(&self) -> Result<()> {
        *self.connected.write().await = false;

        // 남은 응답 채널 정리
        let mut channels = self.response_channels.lock().await;
        for (_, channel) in channels.drain() {
            let _ = channel
                .send(Err(anyhow::anyhow!("Connection closed")))
                .await;
        }

        Ok(())
    }

    async fn send_command(&self, command: Command) -> Result<Response> {
        if !self.is_connected() {
            return Err(anyhow::anyhow!("Not connected"));
        }

        // 요청 ID 생성
        let request_id = {
            let mut id = self.next_request_id.lock().await;
            *id += 1;
            *id
        };

        // 응답 채널 생성
        let (tx, mut rx) = mpsc::channel(1);
        {
            let mut channels = self.response_channels.lock().await;
            channels.insert(request_id, tx);
        }

        // 명령 직렬화 및 전송
        let payload = serde_json::to_string(&command).context("Failed to serialize command")?;

        let message = format!("{},{},{}\n", request_id, self.client_id, payload);

        {
            let mut writer = self.writer.lock().await;
            writer
                .write_all(message.as_bytes())
                .await
                .context("Failed to write command")?;
            writer.flush().await.context("Failed to flush writer")?;
        }

        // 응답 대기
        match rx.recv().await {
            Some(result) => result,
            None => Err(anyhow::anyhow!("Response channel closed")),
        }
    }

    async fn response_loop(self) {
        let mut buffer = String::new();

        loop {
            if !self.is_connected() {
                break;
            }

            // 응답 읽기
            let read_result = {
                let mut reader = self.reader.lock().await;
                reader.read_to_string(&mut buffer).await
            };

            match read_result {
                Ok(0) => {
                    // 연결 종료됨
                    *self.connected.write().await = false;
                    break;
                }
                Ok(_) => {
                    // 응답 처리
                    for line in buffer.lines() {
                        if let Err(e) = self.process_response(line).await {
                            error!("Error processing response: {}", e);
                        }
                    }
                    buffer.clear();
                }
                Err(e) => {
                    error!("Error reading from socket: {}", e);
                    *self.connected.write().await = false;
                    break;
                }
            }
        }

        // 연결이 종료되면 남은 응답 채널에 오류 전송
        let mut channels = self.response_channels.lock().await;
        for (_, channel) in channels.drain() {
            let _ = channel
                .send(Err(anyhow::anyhow!("Connection closed")))
                .await;
        }
    }

    async fn process_response(&self, line: &str) -> Result<()> {
        // 응답 형식: request_id,status,payload
        let parts: Vec<&str> = line.splitn(3, ',').collect();
        if parts.len() != 3 {
            return Err(anyhow::anyhow!("Invalid response format"));
        }

        let request_id: u64 = parts[0].parse().context("Invalid request ID")?;
        let status = parts[1];
        let payload = parts[2];

        // 응답 객체 생성
        let response = match status {
            "success" => {
                let data =
                    serde_json::from_str(payload).context("Failed to parse response data")?;
                Response::Success(data)
            }
            "error" => {
                let error: ErrorResponse =
                    serde_json::from_str(payload).context("Failed to parse error response")?;
                Response::Error(error)
            }
            _ => {
                return Err(anyhow::anyhow!("Unknown response status: {}", status));
            }
        };

        // 응답 채널 찾기
        let tx = {
            let mut channels = self.response_channels.lock().await;
            channels.remove(&request_id)
        };

        // 응답 전송
        if let Some(tx) = tx {
            if let Err(e) = tx.send(Ok(response)).await {
                error!("Failed to send response to channel: {}", e);
            }
        } else {
            warn!("Received response for unknown request ID: {}", request_id);
        }

        Ok(())
    }
}

impl Clone for ClientConnection {
    fn clone(&self) -> Self {
        Self {
            reader: Arc::clone(&self.reader),
            writer: Arc::clone(&self.writer),
            client_id: self.client_id.clone(),
            connected: Arc::clone(&self.connected),
            next_request_id: Arc::clone(&self.next_request_id),
            response_channels: Arc::clone(&self.response_channels),
        }
    }
}
