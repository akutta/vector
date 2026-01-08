//! RowBinary format serialization for ClickHouse.
//!
//! This module implements the RowBinaryWithNamesAndTypes format as specified by ClickHouse.
//! The format consists of:
//! 1. A header containing column names and types
//! 2. Binary-encoded row data
//!
//! Reference: https://clickhouse.com/docs/en/interfaces/formats/RowBinaryWithNamesAndTypes

mod convert;
mod encoder;
mod error;
mod schema;
mod serialize;
mod serializer;
mod types;

// Re-export public types and functions
pub use encoder::RowBinaryEncoder;
pub use error::RowBinaryError;
pub use schema::{SchemaError, SchemaFetcher, TableSchema};
pub use serialize::{serialize_value, write_bytes, write_string, write_varint};
pub use serializer::RowBinarySerializer;
pub use types::ClickHouseType;
