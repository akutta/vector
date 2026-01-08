//! Schema fetching and Arrow schema construction for ClickHouse tables.
//!
//! This module uses the shared schema fetcher and converts the result to an Arrow schema.

use arrow::datatypes::{Field, Schema};
use async_trait::async_trait;
use http::Uri;
use vector_lib::codecs::encoding::format::{ArrowEncodingError, SchemaProvider};

use crate::http::{Auth, HttpClient};
use crate::sinks::clickhouse::schema::{SchemaFetcher, TableSchema};

use super::parser::clickhouse_type_to_arrow;

/// Converts a ClickHouse TableSchema to an Arrow Schema.
fn table_schema_to_arrow(table_schema: &TableSchema) -> crate::Result<Schema> {
    let mut fields = Vec::with_capacity(table_schema.column_order.len());

    for column_name in &table_schema.column_order {
        let column = table_schema
            .get_column(column_name)
            .ok_or_else(|| format!("Column '{}' not found in schema", column_name))?;

        let (arrow_type, nullable) = clickhouse_type_to_arrow(&column.column_type)
            .map_err(|e| format!("Failed to convert column '{}': {}", column_name, e))?;

        fields.push(Field::new(column_name, arrow_type, nullable));
    }

    if fields.is_empty() {
        return Err("No columns found in table schema".into());
    }

    Ok(Schema::new(fields))
}

/// Schema provider implementation for ClickHouse tables.
#[derive(Clone)]
pub struct ClickHouseSchemaProvider {
    fetcher: SchemaFetcher,
    database: String,
    table: String,
}

impl std::fmt::Debug for ClickHouseSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseSchemaProvider")
            .field("database", &self.database)
            .field("table", &self.table)
            .finish()
    }
}

impl ClickHouseSchemaProvider {
    /// Create a new ClickHouse schema provider.
    pub fn new(
        client: HttpClient,
        endpoint: String,
        database: String,
        table: String,
        auth: Option<Auth>,
    ) -> Self {
        let uri: Uri = endpoint.parse().expect("endpoint should be a valid URI");
        let fetcher = SchemaFetcher::new(client, uri, auth);

        Self {
            fetcher,
            database,
            table,
        }
    }
}

#[async_trait]
impl SchemaProvider for ClickHouseSchemaProvider {
    async fn get_schema(&self) -> Result<Schema, ArrowEncodingError> {
        let table_schema = self
            .fetcher
            .fetch_schema(&self.database, &self.table)
            .await
            .map_err(|e| ArrowEncodingError::SchemaFetchError {
                message: e.to_string(),
            })?;

        table_schema_to_arrow(&table_schema).map_err(|e| ArrowEncodingError::SchemaFetchError {
            message: e.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, TimeUnit};
    use std::collections::HashMap;

    use crate::sinks::clickhouse::schema::ColumnInfo;

    fn create_test_schema(columns: Vec<(&str, &str)>) -> TableSchema {
        let mut schema_columns = HashMap::new();
        let mut column_order = Vec::new();

        for (name, col_type) in columns {
            let is_nullable = col_type.starts_with("Nullable(") || col_type.contains("Nullable(");
            schema_columns.insert(
                name.to_string(),
                ColumnInfo {
                    name: name.to_string(),
                    column_type: col_type.to_string(),
                    default_expression: None,
                    is_nullable,
                },
            );
            column_order.push(name.to_string());
        }

        TableSchema {
            database: "test".to_string(),
            table: "test_table".to_string(),
            columns: schema_columns,
            column_order,
        }
    }

    #[test]
    fn test_table_schema_to_arrow() {
        let table_schema = create_test_schema(vec![
            ("id", "Int64"),
            ("message", "String"),
            ("timestamp", "DateTime"),
        ]);

        let schema = table_schema_to_arrow(&table_schema).unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).name(), "message");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).name(), "timestamp");
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Second, None)
        );
    }

    #[test]
    fn test_table_schema_to_arrow_with_type_parameters() {
        let table_schema = create_test_schema(vec![
            ("bytes_sent", "Decimal(18, 2)"),
            ("timestamp", "DateTime64(6)"),
            ("duration_ms", "Decimal32(4)"),
        ]);

        let schema = table_schema_to_arrow(&table_schema).unwrap();
        assert_eq!(schema.fields().len(), 3);

        // Check Decimal parsed from type string
        assert_eq!(schema.field(0).name(), "bytes_sent");
        assert_eq!(schema.field(0).data_type(), &DataType::Decimal128(18, 2));

        // Check DateTime64 parsed from type string
        assert_eq!(schema.field(1).name(), "timestamp");
        assert_eq!(
            schema.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );

        // Check Decimal32 parsed from type string
        assert_eq!(schema.field(2).name(), "duration_ms");
        assert_eq!(schema.field(2).data_type(), &DataType::Decimal128(9, 4));
    }

    #[test]
    fn test_schema_field_ordering() {
        let table_schema = create_test_schema(vec![
            ("timestamp", "DateTime64(3)"),
            ("host", "String"),
            ("message", "String"),
            ("id", "Int64"),
            ("score", "Float64"),
            ("active", "Bool"),
            ("name", "String"),
        ]);

        let schema = table_schema_to_arrow(&table_schema).unwrap();
        assert_eq!(schema.fields().len(), 7);

        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(schema.field(1).name(), "host");
        assert_eq!(schema.field(2).name(), "message");
        assert_eq!(schema.field(3).name(), "id");
        assert_eq!(schema.field(4).name(), "score");
        assert_eq!(schema.field(5).name(), "active");
        assert_eq!(schema.field(6).name(), "name");

        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(3).data_type(), &DataType::Int64);
        assert_eq!(schema.field(4).data_type(), &DataType::Float64);
        assert_eq!(schema.field(5).data_type(), &DataType::Boolean);
    }
}
