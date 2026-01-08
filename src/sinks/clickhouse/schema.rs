//! Common schema handling for the ClickHouse sink.
//!
//! This module provides shared functionality for fetching table schema from ClickHouse.
//! Both Arrow and RowBinary format implementations use this common schema infrastructure.

use std::collections::HashMap;

use bytes::{Buf, Bytes};
use http::{Request, StatusCode, Uri};
use http_body::Body as HttpBody;
use hyper::Body;
use serde::Deserialize;
use snafu::{ResultExt, Snafu};

use crate::http::{Auth, HttpClient};

/// Errors that can occur during schema operations.
#[derive(Debug, Snafu)]
pub enum SchemaError {
    #[snafu(display("Failed to fetch table schema: {}", source))]
    FetchFailed { source: crate::http::HttpError },

    #[snafu(display("ClickHouse returned error status {}: {}", status, body))]
    ClickhouseError { status: StatusCode, body: String },

    #[snafu(display("Failed to parse schema response: {}", source))]
    ParseFailed { source: serde_json::Error },

    #[snafu(display("Table '{}' has no columns", table))]
    EmptyTable { table: String },
}

/// Represents a column in a ClickHouse table.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    /// ClickHouse type (e.g., "String", "Int64", "DateTime64(9)")
    pub column_type: String,
    /// Default expression, if any
    pub default_expression: Option<String>,
    /// Whether the column is nullable
    pub is_nullable: bool,
}

impl ColumnInfo {
    /// Check if this column type is nullable (either explicit Nullable() or has a default).
    #[allow(clippy::missing_const_for_fn)] // Cannot be const: takes &self and accesses instance fields
    pub fn can_be_omitted(&self) -> bool {
        self.is_nullable || self.default_expression.is_some()
    }
}

/// Schema information for a ClickHouse table.
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Columns in the table, keyed by column name
    pub columns: HashMap<String, ColumnInfo>,
    /// Column names in order
    pub column_order: Vec<String>,
}

impl TableSchema {
    /// Get a column by name.
    pub fn get_column(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns.get(name)
    }

    /// Check if a column exists.
    pub fn has_column(&self, name: &str) -> bool {
        self.columns.contains_key(name)
    }
}

/// Response format from ClickHouse DESCRIBE TABLE query with JSON format.
#[derive(Debug, Deserialize)]
struct DescribeTableResponse {
    data: Vec<DescribeTableRow>,
}

#[derive(Debug, Deserialize)]
struct DescribeTableRow {
    name: String,
    #[serde(rename = "type")]
    column_type: String,
    default_expression: Option<String>,
}

/// Fetches the schema for a ClickHouse table.
#[derive(Clone)]
pub struct SchemaFetcher {
    client: HttpClient,
    endpoint: Uri,
    auth: Option<Auth>,
}

impl SchemaFetcher {
    /// Create a new schema fetcher.
    #[allow(clippy::missing_const_for_fn)] // Cannot be const: takes non-const parameters
    pub fn new(client: HttpClient, endpoint: Uri, auth: Option<Auth>) -> Self {
        Self {
            client,
            endpoint,
            auth,
        }
    }

    /// Fetch the schema for a table.
    pub async fn fetch_schema(
        &self,
        database: &str,
        table: &str,
    ) -> Result<TableSchema, SchemaError> {
        let query = format!(
            "DESCRIBE TABLE \"{}\".\"{}\" FORMAT JSON",
            database.replace('"', "\\\""),
            table.replace('"', "\\\"")
        );

        let uri = self.build_query_uri(&query)?;

        let mut request = Request::get(&uri)
            .body(Body::empty())
            .expect("building request should not fail");

        if let Some(auth) = &self.auth {
            auth.apply(&mut request);
        }

        let response = self.client.send(request).await.context(FetchFailedSnafu)?;

        let status = response.status();
        let mut body = response
            .into_body()
            .collect()
            .await
            .map_err(|e| SchemaError::FetchFailed {
                source: crate::http::HttpError::CallRequest { source: e },
            })?
            .aggregate();
        let body_bytes = body.copy_to_bytes(body.remaining());

        if !status.is_success() {
            let body = String::from_utf8_lossy(&body_bytes).to_string();
            return Err(SchemaError::ClickhouseError { status, body });
        }

        self.parse_schema_response(database, table, &body_bytes)
    }

    fn build_query_uri(&self, query: &str) -> Result<String, SchemaError> {
        let encoded_query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("query", query)
            .finish();

        let mut uri = self.endpoint.to_string();
        if !uri.ends_with('/') {
            uri.push('/');
        }
        uri.push('?');
        uri.push_str(&encoded_query);

        Ok(uri)
    }

    fn parse_schema_response(
        &self,
        database: &str,
        table: &str,
        body: &Bytes,
    ) -> Result<TableSchema, SchemaError> {
        let response: DescribeTableResponse =
            serde_json::from_slice(body).context(ParseFailedSnafu)?;

        if response.data.is_empty() {
            return Err(SchemaError::EmptyTable {
                table: table.to_string(),
            });
        }

        let mut columns = HashMap::new();
        let mut column_order = Vec::new();

        for row in response.data {
            let is_nullable =
                row.column_type.starts_with("Nullable(") || row.column_type.contains("Nullable(");

            let column_info = ColumnInfo {
                name: row.name.clone(),
                column_type: row.column_type,
                default_expression: row.default_expression.filter(|s| !s.is_empty()),
                is_nullable,
            };

            column_order.push(row.name.clone());
            columns.insert(row.name, column_info);
        }

        Ok(TableSchema {
            database: database.to_string(),
            table: table.to_string(),
            columns,
            column_order,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_can_be_omitted() {
        let nullable_col = ColumnInfo {
            name: "test".to_string(),
            column_type: "Nullable(String)".to_string(),
            default_expression: None,
            is_nullable: true,
        };
        assert!(nullable_col.can_be_omitted());

        let default_col = ColumnInfo {
            name: "test".to_string(),
            column_type: "String".to_string(),
            default_expression: Some("'default'".to_string()),
            is_nullable: false,
        };
        assert!(default_col.can_be_omitted());

        let required_col = ColumnInfo {
            name: "test".to_string(),
            column_type: "String".to_string(),
            default_expression: None,
            is_nullable: false,
        };
        assert!(!required_col.can_be_omitted());
    }
}

