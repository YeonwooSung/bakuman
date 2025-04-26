use crate::storage::Storage;
use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{Client, config::Region};
use bytes::Bytes;
use std::error::Error;

pub struct S3Storage {
    client: Client,
    bucket: String,
}

impl S3Storage {
    pub async fn new(region: &str, bucket: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let region_provider = RegionProviderChain::first_try(Region::new(region.to_string()));
        let config = aws_config::from_env().region(region_provider).load().await;
        let client = Client::new(&config);
        
        Ok(Self {
            client,
            bucket: bucket.to_string(),
        })
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn store_raw(&self, key: &str, value: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
        let bytes = Bytes::from(value.to_vec());
        
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(bytes.into())
            .send()
            .await?;
            
        Ok(())
    }
    
    async fn get_raw(&self, key: &str) -> Result<Option<Vec<u8>>, Box<dyn Error + Send + Sync>> {
        let result = self.client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;
            
        match result {
            Ok(response) => {
                let data = response.body.collect().await?;
                let bytes = data.into_bytes();
                Ok(Some(bytes.to_vec()))
            }
            Err(err) => {
                if err.to_string().contains("NoSuchKey") {
                    Ok(None)
                } else {
                    Err(Box::new(err))
                }
            }
        }
    }
    
    async fn delete(&self, key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
            
        Ok(())
    }
    
    async fn list_keys_with_prefix(&self, prefix: &str) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let result = self.client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .send()
            .await?;
            
        let keys = result.contents()
            .unwrap_or_default()
            .iter()
            .filter_map(|obj| obj.key().map(String::from))
            .collect();
            
        Ok(keys)
    }
}
