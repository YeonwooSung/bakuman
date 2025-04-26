#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub storage_type: String,
    pub s3_bucket: String,
    pub s3_region: String,
    pub port: u16,
    pub snapshot_interval: u64,
}
