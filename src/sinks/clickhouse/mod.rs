//! The Clickhouse [`vector_lib::sink::VectorSink`]
//!
//! This module contains the [`vector_lib::sink::VectorSink`] instance that is responsible for
//! taking a stream of [`vector_lib::event::Event`] instances and forwarding them to Clickhouse.
//!
//! ## Formats
//!
//! Events can be sent to ClickHouse using different formats:
//!
//! - **JSON formats** (`JSONEachRow`, `JSONAsObject`, `JSONAsString`): Events are encoded as
//!   newline-delimited JSON and sent via HTTP POST.
//!
//! - **Binary formats** (`RowBinaryWithNamesAndTypes`): Events are encoded in ClickHouse's
//!   efficient binary format. This requires a `schema` configuration to map event fields to
//!   table columns. The table schema is fetched from ClickHouse at startup.
//!
//! This sink only supports logs for now but could support metrics and traces as well in the future.

mod arrow;
pub mod config;
#[cfg(all(test, feature = "clickhouse-integration-tests"))]
mod integration_tests;
mod request_builder;
pub mod rowbinary;
pub mod schema;
mod service;
mod sink;
