use serde_json::Value;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("Invalid schema: {0}")]
    InvalidSchema(String),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Schema not found for topic {0}")]
    SchemaNotFound(String),
}

// JSONSchema를 직접 사용하지 않고 직렬화된 형태로 저장
#[derive(Debug, Clone)]
pub struct SchemaRegistry {
    schemas: HashMap<String, Value>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    pub fn register_schema(&mut self, topic: &str, schema_json: Value) -> Result<(), SchemaError> {
        // 스키마 유효성 검증을 여기서 수행하지만 직렬화된 형태로 저장
        let _schema = jsonschema::JSONSchema::options()
            .with_draft(jsonschema::Draft::Draft7)
            .compile(&schema_json)
            .map_err(|e| SchemaError::InvalidSchema(e.to_string()))?;

        // 스키마 자체는 저장하지 않고 JSON 형태로 보관
        self.schemas.insert(topic.to_string(), schema_json);
        Ok(())
    }

    pub fn validate(&self, topic: &str, message: &Value) -> Result<(), SchemaError> {
        let schema_json = self
            .schemas
            .get(topic)
            .ok_or_else(|| SchemaError::SchemaNotFound(topic.to_string()))?;

        // 검증 시에는 스키마를 다시 컴파일
        let schema = jsonschema::JSONSchema::options()
            .with_draft(jsonschema::Draft::Draft7)
            .compile(schema_json)
            .map_err(|e| SchemaError::InvalidSchema(e.to_string()))?;

        schema.validate(message).map_err(|errors| {
            let error_messages: Vec<String> = errors.map(|e| e.to_string()).collect();
            SchemaError::ValidationError(error_messages.join(", "))
        })?;

        Ok(())
    }
}
