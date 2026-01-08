//! RowBinary encoder for converting events to ClickHouse binary format.

use std::{collections::HashMap, io::Write};

use bytes::Bytes;
use ordered_float::NotNan;
use vector_lib::event::{Event, LogEvent, Value};
use vrl::path::{OwnedTargetPath, parse_target_path};

use crate::sinks::clickhouse::config::{OnMissingField, SchemaConfig};

use super::schema::TableSchema;

use super::{
    error::RowBinaryError,
    serialize::{serialize_value, write_string, write_varint},
    types::ClickHouseType,
};

/// Configuration for the RowBinary encoder.
#[derive(Debug, Clone)]
pub struct RowBinaryEncoder {
    /// Column names in order
    pub column_names: Vec<String>,
    /// Column types in order (parsed)
    pub column_types: Vec<ClickHouseType>,
    /// Column type strings (original)
    pub column_type_strings: Vec<String>,
    /// Pre-parsed VRL paths for each column (for O(1) lookup without per-event parsing)
    /// Vec indexed by column index, None if no field mapping for that column
    column_field_paths: Vec<Option<OwnedTargetPath>>,
    /// Behavior for missing fields
    pub on_missing_field: OnMissingField,
    /// Default values for missing fields (by column index)
    pub defaults: HashMap<usize, Value>,
    /// Pre-computed header bytes (column names and types) to avoid recomputing for each batch
    header_bytes: Bytes,
}

impl RowBinaryEncoder {
    /// Create a new RowBinaryEncoder from a table schema and schema config.
    pub fn new(schema: &TableSchema, config: &SchemaConfig) -> Result<Self, RowBinaryError> {
        let mut column_names = Vec::new();
        let mut column_types = Vec::new();
        let mut column_type_strings = Vec::new();
        let mut column_field_paths = Vec::new();
        let mut defaults = HashMap::new();

        // Process columns in order
        for column_name in &schema.column_order {
            let column_info = schema.columns.get(column_name).ok_or_else(|| {
                RowBinaryError::SerializationError {
                    message: format!("Column {} not found in schema", column_name),
                }
            })?;

            // Check if this column has a server-side default expression (like NOW(), uuid(), etc.)
            // These are function calls that should be evaluated by ClickHouse, not Vector
            let has_server_side_default = column_info
                .default_expression
                .as_ref()
                .map(|expr| is_server_side_default(expr))
                .unwrap_or(false);

            // Skip columns with server-side defaults that don't have explicit defaults configured
            // This lets ClickHouse use its DEFAULT expression (e.g., NOW() for _inserted_at)
            if has_server_side_default && !config.defaults.contains_key(column_name) {
                continue;
            }

            let ch_type = ClickHouseType::parse(&column_info.column_type)?;

            column_names.push(column_name.clone());
            column_types.push(ch_type.clone());
            column_type_strings.push(column_info.column_type.clone());

            // Automatically map column name to event field path (column "host" -> ".host")
            // Pre-parse the field path for this column (avoids per-event parsing overhead)
            match parse_target_path(column_name) {
                Ok(parsed_path) => column_field_paths.push(Some(parsed_path)),
                Err(_) => {
                    return Err(RowBinaryError::SerializationError {
                        message: format!("Invalid column name for field path: {}", column_name),
                    });
                }
            }

            // Set up default values
            let column_idx = column_names.len() - 1;
            if let Some(default_str) = config.defaults.get(column_name) {
                let default_value = parse_default_value(default_str, &ch_type)?;
                defaults.insert(column_idx, default_value);
            } else if column_info.can_be_omitted() {
                // Use type's default value for nullable/default columns
                defaults.insert(column_idx, ch_type.default_value());
            }
        }

        let mut header_buffer = Vec::new();
        write_varint(&mut header_buffer, column_names.len() as u64)?;
        for name in &column_names {
            write_string(&mut header_buffer, name)?;
        }
        for type_str in &column_type_strings {
            write_string(&mut header_buffer, type_str)?;
        }
        let header_bytes = Bytes::from(header_buffer);

        Ok(Self {
            column_names,
            column_types,
            column_type_strings,
            column_field_paths,
            on_missing_field: config.on_missing_field,
            defaults,
            header_bytes,
        })
    }

    /// Returns true if any column in the schema is a JSON type.
    /// This is used to determine if we need to set input_format_binary_read_json_as_string=1.
    pub fn has_json_columns(&self) -> bool {
        self.column_types
            .iter()
            .any(|t| matches!(t, ClickHouseType::JSON))
    }

    /// Write the header (column names and types) to the buffer.
    pub fn write_header<W: Write>(&self, writer: &mut W) -> Result<(), RowBinaryError> {
        writer.write_all(&self.header_bytes)?;
        Ok(())
    }

    /// Encode a single event to RowBinary format.
    pub fn encode_event<W: Write>(
        &self,
        writer: &mut W,
        event: &Event,
    ) -> Result<(), RowBinaryError> {
        let log = match event {
            Event::Log(log) => log,
            _ => {
                return Err(RowBinaryError::SerializationError {
                    message: "Only log events are supported".to_string(),
                });
            }
        };

        // For each column, get the value from the event or use default
        for (col_idx, ch_type) in self.column_types.iter().enumerate() {
            let value = self.get_column_value(log, col_idx)?;
            serialize_value(writer, &value, ch_type)?;
        }

        Ok(())
    }

    /// Get the value for a column from a log event.
    fn get_column_value(&self, log: &LogEvent, col_idx: usize) -> Result<Value, RowBinaryError> {
        if let Some(Some(parsed_path)) = self.column_field_paths.get(col_idx) {
            if let Some(value) = log.get(parsed_path) {
                return Ok(value.clone());
            }
        }
        match self.on_missing_field {
            OnMissingField::UseDefault => {
                if let Some(default) = self.defaults.get(&col_idx) {
                    Ok(default.clone())
                } else {
                    Ok(self.column_types[col_idx].default_value())
                }
            }
            OnMissingField::InsertNull => {
                // Check if the column is nullable
                if matches!(self.column_types[col_idx], ClickHouseType::Nullable(_)) {
                    Ok(Value::Null)
                } else {
                    Err(RowBinaryError::MissingField {
                        field: self.column_names[col_idx].clone(),
                    })
                }
            }
            OnMissingField::DropEvent => Err(RowBinaryError::MissingField {
                field: self.column_names[col_idx].clone(),
            }),
        }
    }

    /// Encode multiple events, returning the complete binary payload.
    pub fn encode_batch(&self, events: &[Event]) -> Result<Bytes, RowBinaryError> {
        let estimated_row_size = 100;
        let mut buffer = Vec::with_capacity(
            self.header_bytes.len() + (estimated_row_size * events.len())
        );

        buffer.extend_from_slice(&self.header_bytes);

        // Write each row
        for event in events {
            self.encode_event(&mut buffer, event)?;
        }

        Ok(Bytes::from(buffer))
    }
}

/// Check if a default expression is a server-side default that should be evaluated by ClickHouse.
/// Server-side defaults include function calls like NOW(), uuid(), currentDatabase(), etc.
/// These should NOT be replaced with Vector-generated values; instead, the column should be
/// omitted from the INSERT so ClickHouse evaluates the expression.
fn is_server_side_default(expr: &str) -> bool {
    let expr = expr.trim();

    // Function calls contain parentheses, e.g., NOW(), uuid(), generateUUIDv4()
    if expr.contains('(') && expr.contains(')') {
        return true;
    }

    // Common ClickHouse server-side expressions (case-insensitive)
    let expr_lower = expr.to_lowercase();
    matches!(
        expr_lower.as_str(),
        "now" | "today" | "yesterday" | "currentdatabase" | "currentuser"
    )
}

/// Parse a default value string into a Value based on the ClickHouse type.
fn parse_default_value(
    default_str: &str,
    ch_type: &ClickHouseType,
) -> Result<Value, RowBinaryError> {
    match ch_type {
        ClickHouseType::Int8
        | ClickHouseType::Int16
        | ClickHouseType::Int32
        | ClickHouseType::Int64
        | ClickHouseType::Int128
        | ClickHouseType::Int256
        | ClickHouseType::UInt8
        | ClickHouseType::UInt16
        | ClickHouseType::UInt32
        | ClickHouseType::UInt64
        | ClickHouseType::UInt128
        | ClickHouseType::UInt256 => {
            let v: i64 = default_str
                .parse()
                .map_err(|_| RowBinaryError::SerializationError {
                    message: format!("Invalid default integer value: {}", default_str),
                })?;
            Ok(Value::Integer(v))
        }
        ClickHouseType::Float32 | ClickHouseType::Float64 => {
            let v: f64 = default_str
                .parse()
                .map_err(|_| RowBinaryError::SerializationError {
                    message: format!("Invalid default float value: {}", default_str),
                })?;
            Ok(Value::Float(NotNan::new(v).map_err(|_| {
                RowBinaryError::SerializationError {
                    message: "NaN is not a valid default value".to_string(),
                }
            })?))
        }
        ClickHouseType::Bool => {
            let v = match default_str.to_lowercase().as_str() {
                "true" | "1" | "yes" => true,
                "false" | "0" | "no" => false,
                _ => {
                    return Err(RowBinaryError::SerializationError {
                        message: format!("Invalid default boolean value: {}", default_str),
                    });
                }
            };
            Ok(Value::Boolean(v))
        }
        ClickHouseType::String | ClickHouseType::FixedString(_) => {
            Ok(Value::Bytes(Bytes::from(default_str.to_string())))
        }
        ClickHouseType::Nullable(inner) => {
            if default_str.to_lowercase() == "null" {
                Ok(Value::Null)
            } else {
                parse_default_value(default_str, inner)
            }
        }
        ClickHouseType::LowCardinality(inner) => parse_default_value(default_str, inner),
        _ => {
            // For other types, just use the string representation
            Ok(Value::Bytes(Bytes::from(default_str.to_string())))
        }
    }
}
