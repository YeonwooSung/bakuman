use bakuman_client::{producer::Producer, BakumanClient, Client, ClientConfig, Message};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

// 주문 상태
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderState {
    order_id: String,
    customer_id: String,
    amount: f64,
    items: Vec<OrderItem>,
    status: String,
    shipping_address: Option<String>,
    created_at: String,
    updated_at: String,
}

// 주문 항목
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderItem {
    product_id: String,
    quantity: u32,
    price: f64,
}

// 주문 이벤트 열거형
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
enum OrderEvent {
    OrderCreated(OrderCreated),
    OrderItemAdded(OrderItemAdded),
    OrderItemRemoved(OrderItemRemoved),
    OrderAddressUpdated(OrderAddressUpdated),
    OrderStatusChanged(OrderStatusChanged),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderCreated {
    order_id: String,
    customer_id: String,
    created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderItemAdded {
    product_id: String,
    quantity: u32,
    price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderItemRemoved {
    product_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderAddressUpdated {
    shipping_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderStatusChanged {
    new_status: String,
    reason: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 로깅 설정
    tracing_subscriber::fmt::init();

    // 클라이언트 설정
    let config = ClientConfig {
        hosts: vec!["localhost:9092".to_string()],
        client_id: "event-sourcing-example".to_string(),
        connection_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(30),
        connection_retry_max_attempts: 3,
        connection_retry_base_delay: Duration::from_millis(100),
    };

    // 클라이언트 생성 및 연결
    let client = Arc::new(BakumanClient::new(config));
    client.connect().await?;

    println!("Connected to Bakuman server");

    // 프로듀서 생성
    let producer = Producer::new(client.clone()).with_default_topic("order-events");

    // 주문 생성 이벤트
    let order_created = OrderEvent::OrderCreated(OrderCreated {
        order_id: "order-789".to_string(),
        customer_id: "customer-456".to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
    });

    // 이벤트 발행
    let mut version = 0;
    let result = producer.publish("order-789", order_created, None).await?;
    version = result.version.unwrap_or(0);
    println!("OrderCreated event published with version: {}", version);

    // 주문 항목 추가 이벤트
    let item_added = OrderEvent::OrderItemAdded(OrderItemAdded {
        product_id: "product-101".to_string(),
        quantity: 2,
        price: 29.99,
    });

    let result = producer
        .publish_with_version("order-789", item_added, None, version)
        .await?;
    version = result.version.unwrap_or(0);
    println!("OrderItemAdded event published with version: {}", version);

    // 주문 상태 변경 이벤트
    let status_changed = OrderEvent::OrderStatusChanged(OrderStatusChanged {
        new_status: "processing".to_string(),
        reason: Some("Payment confirmed".to_string()),
    });

    let result = producer
        .publish_with_version("order-789", status_changed, None, version)
        .await?;
    version = result.version.unwrap_or(0);
    println!(
        "OrderStatusChanged event published with version: {}",
        version
    );

    // 배송 주소 업데이트 이벤트
    let address_updated = OrderEvent::OrderAddressUpdated(OrderAddressUpdated {
        shipping_address: "123 Main St, Anytown, AN 12345".to_string(),
    });

    let result = producer
        .publish_with_version("order-789", address_updated, None, version)
        .await?;
    version = result.version.unwrap_or(0);
    println!(
        "OrderAddressUpdated event published with version: {}",
        version
    );

    // 이벤트 히스토리 조회
    let events = client
        .consume_by_key("order-events", "order-789", None)
        .await?;
    println!("Retrieved {} events for order-789", events.len());

    // 이벤트 재생을 통한 현재 상태 구성
    let order_state = replay_events(&events)?;
    println!("\nCurrent Order State:");
    println!("Order ID: {}", order_state.order_id);
    println!("Customer: {}", order_state.customer_id);
    println!("Status: {}", order_state.status);
    println!(
        "Address: {}",
        order_state.shipping_address.unwrap_or_default()
    );
    println!("Items: {}", order_state.items.len());
    println!("Total Amount: ${:.2}", order_state.amount);

    // 클라이언트 종료
    client.close().await?;
    println!("Connection closed");

    Ok(())
}

// 이벤트 재생을 통한 상태 구성
fn replay_events(events: &[Message]) -> anyhow::Result<OrderState> {
    let mut state = None;

    for event in events {
        let event: OrderEvent = serde_json::from_slice(&event.value)?;

        state = Some(match event {
            OrderEvent::OrderCreated(data) => {
                // 초기 상태 생성
                OrderState {
                    order_id: data.order_id,
                    customer_id: data.customer_id,
                    amount: 0.0,
                    items: Vec::new(),
                    status: "created".to_string(),
                    shipping_address: None,
                    created_at: data.created_at,
                    updated_at: data.created_at,
                }
            }
            OrderEvent::OrderItemAdded(data) => {
                let mut current = state.unwrap();

                // 항목 추가 및 금액 계산
                current.items.push(OrderItem {
                    product_id: data.product_id,
                    quantity: data.quantity,
                    price: data.price,
                });

                current.amount = current
                    .items
                    .iter()
                    .map(|item| item.price * item.quantity as f64)
                    .sum();

                current.updated_at = chrono::Utc::now().to_rfc3339();
                current
            }
            OrderEvent::OrderItemRemoved(data) => {
                let mut current = state.unwrap();

                // 항목 제거
                current
                    .items
                    .retain(|item| item.product_id != data.product_id);

                current.amount = current
                    .items
                    .iter()
                    .map(|item| item.price * item.quantity as f64)
                    .sum();

                current.updated_at = chrono::Utc::now().to_rfc3339();
                current
            }
            OrderEvent::OrderAddressUpdated(data) => {
                let mut current = state.unwrap();

                // 주소 업데이트
                current.shipping_address = Some(data.shipping_address);
                current.updated_at = chrono::Utc::now().to_rfc3339();
                current
            }
            OrderEvent::OrderStatusChanged(data) => {
                let mut current = state.unwrap();

                // 상태 변경
                current.status = data.new_status;
                current.updated_at = chrono::Utc::now().to_rfc3339();
                current
            }
        });
    }

    state.ok_or_else(|| anyhow::anyhow!("No events to replay"))
}
