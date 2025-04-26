// examples/simple_consumer.rs
use async_trait::async_trait;
use bakuman_client::consumer::MessageHandler; // MessageHandler 트레이트를 consumer 모듈에서 가져옴
use bakuman_client::{BakumanClient, Client, ClientConfig, Message};
use std::sync::Arc;
use std::time::Duration;

struct SimpleMessageHandler;

#[async_trait]
impl MessageHandler for SimpleMessageHandler {
    async fn handle(&self, message: Message) -> anyhow::Result<()> {
        let payload = String::from_utf8_lossy(&message.value);
        println!("Received message: Key={}, Value={}", message.key, payload);
        println!("  Version: {:?}, Topic: {}", message.version, message.topic);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 로깅 설정
    tracing_subscriber::fmt::init();

    // 클라이언트 설정
    let config = ClientConfig {
        hosts: vec!["localhost:9092".to_string()],
        client_id: "simple-consumer-example".to_string(),
        connection_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(30),
        connection_retry_max_attempts: 3,
        connection_retry_base_delay: Duration::from_millis(100),
    };

    // 클라이언트 생성 및 연결
    let client = Arc::new(BakumanClient::new(config));
    client.connect().await?;

    println!("Connected to CloudKafka server");

    // 메시지 핸들러 생성
    let handler = Arc::new(SimpleMessageHandler);

    // 특정 토픽 구독
    let subscription_id = client.subscribe("orders").await?;
    println!("Subscribed to 'orders' topic with ID: {}", subscription_id);

    // 키별 메시지 조회
    println!("Looking for messages with key 'customer-123'");
    let messages = client
        .consume_by_key("orders", "customer-123", None)
        .await?;

    if messages.is_empty() {
        println!("No messages found. Waiting for new messages...");
    } else {
        println!("Found {} existing messages:", messages.len());

        for msg in messages {
            let payload = String::from_utf8_lossy(&msg.value);
            println!("  Message: {} (version: {:?})", payload, msg.version);
        }
    }

    // 새 메시지 대기 (Ctrl+C로 종료할 때까지)
    println!("\nPress Ctrl+C to exit");
    tokio::signal::ctrl_c().await?;

    // 구독 취소
    client.unsubscribe(subscription_id).await?;
    println!("Unsubscribed from topic");

    // 클라이언트 종료
    client.close().await?;
    println!("Connection closed");

    Ok(())
}
