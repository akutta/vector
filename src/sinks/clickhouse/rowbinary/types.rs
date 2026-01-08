//! ClickHouse type system for RowBinary encoding.
//!
//! Re-exports the canonical `ClickHouseType` from `type_parser` and adds
//! RowBinary-specific functionality like default values.

use bytes::Bytes;
use ordered_float::NotNan;
use vector_lib::event::{ObjectMap, Value};

// Re-export the canonical type from type_parser
pub use crate::sinks::clickhouse::type_parser::ClickHouseType;

/// Extension trait for RowBinary-specific functionality.
impl ClickHouseType {
    /// Get the default value for this type (used when fields are missing).
    pub fn default_value(&self) -> Value {
        match self {
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
            | ClickHouseType::UInt256 => Value::Integer(0),
            ClickHouseType::Float32 | ClickHouseType::Float64 => {
                Value::Float(NotNan::new(0.0).unwrap())
            }
            ClickHouseType::Bool => Value::Boolean(false),
            ClickHouseType::String | ClickHouseType::FixedString(_) => {
                Value::Bytes(Bytes::from(""))
            }
            ClickHouseType::Date | ClickHouseType::Date32 => Value::Integer(0),
            ClickHouseType::DateTime => Value::Integer(0),
            ClickHouseType::DateTime64 { .. } => Value::Integer(0),
            ClickHouseType::UUID => {
                Value::Bytes(Bytes::from("00000000-0000-0000-0000-000000000000"))
            }
            ClickHouseType::IPv4 => Value::Bytes(Bytes::from("0.0.0.0")),
            ClickHouseType::IPv6 => Value::Bytes(Bytes::from("::")),
            ClickHouseType::Decimal { .. }
            | ClickHouseType::Decimal32 { .. }
            | ClickHouseType::Decimal64 { .. }
            | ClickHouseType::Decimal128 { .. }
            | ClickHouseType::Decimal256 { .. } => Value::Float(NotNan::new(0.0).unwrap()),
            ClickHouseType::Nullable(inner) => inner.default_value(),
            ClickHouseType::Array(_) => Value::Array(vec![]),
            ClickHouseType::LowCardinality(inner) => inner.default_value(),
            ClickHouseType::Map { .. } => Value::Object(ObjectMap::new()),
            ClickHouseType::Tuple(types) => {
                Value::Array(types.iter().map(|t| t.default_value()).collect())
            }
            ClickHouseType::Enum8(_) | ClickHouseType::Enum16(_) => Value::Integer(0),
            ClickHouseType::JSON => Value::Bytes(Bytes::from("{}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_types() {
        assert_eq!(ClickHouseType::parse("Int64").unwrap(), ClickHouseType::Int64);
        assert_eq!(ClickHouseType::parse("String").unwrap(), ClickHouseType::String);
    }

    #[test]
    fn test_parse_nullable() {
        assert_eq!(
            ClickHouseType::parse("Nullable(String)").unwrap(),
            ClickHouseType::Nullable(Box::new(ClickHouseType::String))
        );
    }

    #[test]
    fn test_parse_array() {
        assert_eq!(
            ClickHouseType::parse("Array(Int32)").unwrap(),
            ClickHouseType::Array(Box::new(ClickHouseType::Int32))
        );
    }

    #[test]
    fn test_default_values() {
        assert_eq!(ClickHouseType::Int64.default_value(), Value::Integer(0));
        assert_eq!(ClickHouseType::String.default_value(), Value::Bytes(Bytes::from("")));
        assert_eq!(ClickHouseType::Bool.default_value(), Value::Boolean(false));
    }
}
