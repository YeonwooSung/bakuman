use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod broker;
mod concurrency;
mod model;
mod plugins;
mod storage;

use broker::Broker;
use model::config::BrokerConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 로깅 설정
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting Cloud Native Event Log System");

    // 브로커 설정
    let config = BrokerConfig {
        storage_type: "s3".to_string(),
        s3_bucket: "event-log-storage".to_string(),
        s3_region: "us-west-2".to_string(),
        port: 9092,
        snapshot_interval: 1000, // 1000개 메시지마다 스냅샷
    };

    // 브로커 인스턴스 생성
    let broker = Arc::new(Broker::new(config).await?);
    let broker_clone = broker.clone();

    // 브로커 서비스 실행
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
    let server_handle = tokio::spawn(async move {
        broker_clone.run().await.unwrap();
    });

    // Ctrl+C 핸들링
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        info!("Received shutdown signal");
        let _ = shutdown_tx.send(()).await;
    });

    // 종료 신호 대기
    let _ = shutdown_rx.recv().await;
    info!("Shutting down");

    Ok(())
}
