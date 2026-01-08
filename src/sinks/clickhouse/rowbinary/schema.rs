//! Schema handling for the RowBinary format.
//!
//! This module re-exports the common schema types and adds any RowBinary-specific functionality.

// Re-export common schema types used by config.rs and other consumers
pub use crate::sinks::clickhouse::schema::{SchemaError, SchemaFetcher, TableSchema};
