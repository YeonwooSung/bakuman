use bakuman_client::{BakumanClient, Client, ClientConfig, Message};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 로깅 설정
    tracing_subscriber::fmt::init();

    // 클라이언트 설정
    let config = ClientConfig {
        hosts: vec!["localhost:9092".to_string()],
        client_id: "simple-producer-example".to_string(),
        connection_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(30),
        connection_retry_max_attempts: 3,
        connection_retry_base_delay: Duration::from_millis(100),
    };

    // 클라이언트 생성 및 연결
    let client = Arc::new(BakumanClient::new(config));
    client.connect().await?;

    println!("Connected to Bakuman server");

    // 메시지 생성
    let message = Message::new(
        "customer-123",
        r#"{"orderId": "order-456", "amount": 99.95, "status": "completed"}"#,
        "orders",
    );

    // 메시지 발행
    let result = client.publish(message).await?;
    println!("Message published with id: {}", result.id);

    // 동일한 키에 대한 메시지 소비
    let messages = client
        .consume_by_key("orders", "customer-123", None)
        .await?;
    println!("Retrieved {} messages for customer-123", messages.len());

    for msg in messages {
        let payload = String::from_utf8_lossy(&msg.value);
        println!("Message: {} (version: {:?})", payload, msg.version);
    }

    // 클라이언트 종료
    client.close().await?;
    println!("Connection closed");

    Ok(())
}
