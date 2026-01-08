//! RowBinary batch serializer for encoding events in batches.

use bytes::BytesMut;
use std::sync::Arc;
use vector_config::configurable_component;
use vector_lib::{config::DataType, event::Event, schema};

use super::{encoder::RowBinaryEncoder, error::RowBinaryError, schema::TableSchema};
use crate::{codecs::BatchSerializerTrait, sinks::clickhouse::config::SchemaConfig};
use vector_lib::{codecs::encoding::Error, event::Event as VectorEvent};

/// Configuration for RowBinary batch serialization.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)] // May be used in the future for configuration-based serialization
pub struct RowBinarySerializerConfig {
    /// The table schema (fetched from ClickHouse).
    #[serde(skip)]
    pub schema: Option<Arc<TableSchema>>,

    /// Schema configuration (required columns, defaults, etc.).
    #[serde(flatten)]
    pub schema_config: SchemaConfig,

    /// Allow nullable fields to be null even if not explicitly nullable.
    ///
    /// When enabled, fields can be null even if the schema doesn't mark them as nullable.
    /// When disabled, null values for non-nullable fields will cause an error.
    #[serde(default = "default_allow_nullable_fields")]
    pub allow_nullable_fields: bool,
}

#[allow(dead_code)] // May be used in the future
const fn default_allow_nullable_fields() -> bool {
    false
}

#[allow(dead_code)] // May be used in the future
impl RowBinarySerializerConfig {
    /// Build a `RowBinarySerializer` from this configuration.
    pub fn build(
        &self,
    ) -> Result<RowBinarySerializer, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let schema = self.schema.as_ref().ok_or(
            "RowBinary serializer requires a schema. Pass a schema or fetch from provider before creating serializer."
        )?;

        RowBinarySerializer::new(Arc::clone(schema), &self.schema_config)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>)
    }

    /// The data type of events that are accepted by this serializer.
    pub const fn input_type(&self) -> DataType {
        DataType::Log
    }

    /// The schema required by the serializer.
    pub fn schema_requirement(&self) -> schema::Requirement {
        schema::Requirement::empty()
    }
}

/// RowBinary batch serializer that holds the schema and encoder.
#[derive(Clone, Debug)]
pub struct RowBinarySerializer {
    encoder: Arc<RowBinaryEncoder>,
}

impl RowBinarySerializer {
    /// Create a new RowBinarySerializer with the given schema and config.
    pub fn new(
        schema: Arc<TableSchema>,
        schema_config: &SchemaConfig,
    ) -> Result<Self, RowBinaryError> {
        let encoder = RowBinaryEncoder::new(&schema, schema_config)?;
        Ok(Self {
            encoder: Arc::new(encoder),
        })
    }
}

impl tokio_util::codec::Encoder<Vec<Event>> for RowBinarySerializer {
    type Error = RowBinaryError;

    fn encode(&mut self, events: Vec<Event>, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        if events.is_empty() {
            return Err(RowBinaryError::SerializationError {
                message: "No events provided for encoding".to_string(),
            });
        }

        let bytes = self.encoder.encode_batch(&events)?;
        buffer.extend_from_slice(&bytes);
        Ok(())
    }
}

// Implement BatchSerializerTrait for RowBinarySerializer
impl BatchSerializerTrait for RowBinarySerializer {
    fn encode_batch(
        &mut self,
        events: Vec<VectorEvent>,
        buffer: &mut BytesMut,
    ) -> Result<(), Error> {
        tokio_util::codec::Encoder::<Vec<Event>>::encode(self, events, buffer).map_err(|err| {
            // Convert RowBinaryError to io::Error, then to Error
            let io_err = match err {
                RowBinaryError::IoError { source } => source,
                e => std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()),
            };
            Error::SerializingError(Box::new(io_err))
        })
    }

    fn content_type(&self) -> &'static str {
        "application/octet-stream"
    }
}
