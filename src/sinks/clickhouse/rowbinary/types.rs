//! ClickHouse type system and parsing.

use bytes::Bytes;
use ordered_float::NotNan;
use vector_lib::event::{ObjectMap, Value};

use super::error::RowBinaryError;

/// Parsed ClickHouse type representation.
#[derive(Debug, Clone, PartialEq)]
pub enum ClickHouseType {
    // Integer types
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,

    // Floating point types
    Float32,
    Float64,

    // Boolean
    Bool,

    // String types
    String,
    FixedString(usize),

    // Date/Time types
    Date,
    Date32,
    DateTime,
    DateTime64 {
        precision: u8,
        timezone: Option<String>,
    },

    // UUID
    UUID,

    // IPv4/IPv6
    IPv4,
    IPv6,

    // Decimal types
    Decimal {
        precision: u8,
        scale: u8,
    },
    Decimal32 {
        scale: u8,
    },
    Decimal64 {
        scale: u8,
    },
    Decimal128 {
        scale: u8,
    },
    Decimal256 {
        scale: u8,
    },

    // Nullable wrapper
    Nullable(Box<ClickHouseType>),

    // Array type
    Array(Box<ClickHouseType>),

    // LowCardinality wrapper
    LowCardinality(Box<ClickHouseType>),

    // Map type
    Map {
        key: Box<ClickHouseType>,
        value: Box<ClickHouseType>,
    },

    // Tuple type
    Tuple(Vec<ClickHouseType>),

    // Enum types
    Enum8(Vec<(String, i8)>),
    Enum16(Vec<(String, i16)>),

    // JSON type (serialized as String in RowBinary)
    JSON,
}

impl ClickHouseType {
    /// Parse a ClickHouse type string into its structured representation.
    pub fn parse(type_str: &str) -> Result<Self, RowBinaryError> {
        let type_str = type_str.trim();

        // Handle Nullable wrapper
        if type_str.starts_with("Nullable(") && type_str.ends_with(')') {
            let inner = &type_str[9..type_str.len() - 1];
            return Ok(ClickHouseType::Nullable(Box::new(Self::parse(inner)?)));
        }

        // Handle LowCardinality wrapper
        if type_str.starts_with("LowCardinality(") && type_str.ends_with(')') {
            let inner = &type_str[15..type_str.len() - 1];
            return Ok(ClickHouseType::LowCardinality(Box::new(Self::parse(
                inner,
            )?)));
        }

        // Handle Array type
        if type_str.starts_with("Array(") && type_str.ends_with(')') {
            let inner = &type_str[6..type_str.len() - 1];
            return Ok(ClickHouseType::Array(Box::new(Self::parse(inner)?)));
        }

        // Handle Map type
        if type_str.starts_with("Map(") && type_str.ends_with(')') {
            let inner = &type_str[4..type_str.len() - 1];
            let (key, value) = Self::split_map_types(inner)?;
            return Ok(ClickHouseType::Map {
                key: Box::new(Self::parse(key)?),
                value: Box::new(Self::parse(value)?),
            });
        }

        // Handle Tuple type
        if type_str.starts_with("Tuple(") && type_str.ends_with(')') {
            let inner = &type_str[6..type_str.len() - 1];
            let types = Self::split_tuple_types(inner)?;
            let parsed_types: Result<Vec<_>, _> = types.iter().map(|t| Self::parse(t)).collect();
            return Ok(ClickHouseType::Tuple(parsed_types?));
        }

        // Handle FixedString
        if type_str.starts_with("FixedString(") && type_str.ends_with(')') {
            let len_str = &type_str[12..type_str.len() - 1];
            let len = len_str
                .parse::<usize>()
                .map_err(|_| RowBinaryError::InvalidTypeSpec {
                    spec: type_str.to_string(),
                })?;
            return Ok(ClickHouseType::FixedString(len));
        }

        // Handle DateTime64
        if type_str.starts_with("DateTime64(") && type_str.ends_with(')') {
            let inner = &type_str[11..type_str.len() - 1];
            let parts: Vec<&str> = inner.split(',').collect();
            let precision =
                parts[0]
                    .trim()
                    .parse::<u8>()
                    .map_err(|_| RowBinaryError::InvalidTypeSpec {
                        spec: type_str.to_string(),
                    })?;
            let timezone = if parts.len() > 1 {
                Some(parts[1].trim().trim_matches('\'').to_string())
            } else {
                None
            };
            return Ok(ClickHouseType::DateTime64 {
                precision,
                timezone,
            });
        }

        // Handle DateTime with timezone
        if type_str.starts_with("DateTime(") && type_str.ends_with(')') {
            // DateTime with timezone - treat as basic DateTime for serialization
            return Ok(ClickHouseType::DateTime);
        }

        // Handle Decimal types
        if type_str.starts_with("Decimal(") && type_str.ends_with(')') {
            let inner = &type_str[8..type_str.len() - 1];
            let parts: Vec<&str> = inner.split(',').collect();
            if parts.len() != 2 {
                return Err(RowBinaryError::InvalidTypeSpec {
                    spec: type_str.to_string(),
                });
            }
            let precision =
                parts[0]
                    .trim()
                    .parse::<u8>()
                    .map_err(|_| RowBinaryError::InvalidTypeSpec {
                        spec: type_str.to_string(),
                    })?;
            let scale =
                parts[1]
                    .trim()
                    .parse::<u8>()
                    .map_err(|_| RowBinaryError::InvalidTypeSpec {
                        spec: type_str.to_string(),
                    })?;
            return Ok(ClickHouseType::Decimal { precision, scale });
        }

        if type_str.starts_with("Decimal32(") && type_str.ends_with(')') {
            let scale_str = &type_str[10..type_str.len() - 1];
            let scale =
                scale_str
                    .trim()
                    .parse::<u8>()
                    .map_err(|_| RowBinaryError::InvalidTypeSpec {
                        spec: type_str.to_string(),
                    })?;
            return Ok(ClickHouseType::Decimal32 { scale });
        }

        if type_str.starts_with("Decimal64(") && type_str.ends_with(')') {
            let scale_str = &type_str[10..type_str.len() - 1];
            let scale =
                scale_str
                    .trim()
                    .parse::<u8>()
                    .map_err(|_| RowBinaryError::InvalidTypeSpec {
                        spec: type_str.to_string(),
                    })?;
            return Ok(ClickHouseType::Decimal64 { scale });
        }

        if type_str.starts_with("Decimal128(") && type_str.ends_with(')') {
            let scale_str = &type_str[11..type_str.len() - 1];
            let scale =
                scale_str
                    .trim()
                    .parse::<u8>()
                    .map_err(|_| RowBinaryError::InvalidTypeSpec {
                        spec: type_str.to_string(),
                    })?;
            return Ok(ClickHouseType::Decimal128 { scale });
        }

        if type_str.starts_with("Decimal256(") && type_str.ends_with(')') {
            let scale_str = &type_str[11..type_str.len() - 1];
            let scale =
                scale_str
                    .trim()
                    .parse::<u8>()
                    .map_err(|_| RowBinaryError::InvalidTypeSpec {
                        spec: type_str.to_string(),
                    })?;
            return Ok(ClickHouseType::Decimal256 { scale });
        }

        // Handle Enum types
        if type_str.starts_with("Enum8(") && type_str.ends_with(')') {
            let inner = &type_str[6..type_str.len() - 1];
            let variants = Self::parse_enum_variants::<i8>(inner)?;
            return Ok(ClickHouseType::Enum8(variants));
        }

        if type_str.starts_with("Enum16(") && type_str.ends_with(')') {
            let inner = &type_str[7..type_str.len() - 1];
            let variants = Self::parse_enum_variants::<i16>(inner)?;
            return Ok(ClickHouseType::Enum16(variants));
        }

        // Simple types
        match type_str {
            "Int8" => Ok(ClickHouseType::Int8),
            "Int16" => Ok(ClickHouseType::Int16),
            "Int32" => Ok(ClickHouseType::Int32),
            "Int64" => Ok(ClickHouseType::Int64),
            "Int128" => Ok(ClickHouseType::Int128),
            "Int256" => Ok(ClickHouseType::Int256),
            "UInt8" => Ok(ClickHouseType::UInt8),
            "UInt16" => Ok(ClickHouseType::UInt16),
            "UInt32" => Ok(ClickHouseType::UInt32),
            "UInt64" => Ok(ClickHouseType::UInt64),
            "UInt128" => Ok(ClickHouseType::UInt128),
            "UInt256" => Ok(ClickHouseType::UInt256),
            "Float32" => Ok(ClickHouseType::Float32),
            "Float64" => Ok(ClickHouseType::Float64),
            "Bool" | "Boolean" => Ok(ClickHouseType::Bool),
            "String" => Ok(ClickHouseType::String),
            "Date" => Ok(ClickHouseType::Date),
            "Date32" => Ok(ClickHouseType::Date32),
            "DateTime" => Ok(ClickHouseType::DateTime),
            "UUID" => Ok(ClickHouseType::UUID),
            "IPv4" => Ok(ClickHouseType::IPv4),
            "IPv6" => Ok(ClickHouseType::IPv6),
            "JSON" | "Object('json')" => Ok(ClickHouseType::JSON),
            _ => Err(RowBinaryError::UnsupportedType {
                type_name: type_str.to_string(),
            }),
        }
    }

    /// Split Map(K, V) type arguments.
    fn split_map_types(inner: &str) -> Result<(&str, &str), RowBinaryError> {
        let comma_pos = Self::find_top_level_comma(inner)?;
        let key = &inner[..comma_pos];
        let value = &inner[comma_pos + 1..];
        Ok((key.trim(), value.trim()))
    }

    /// Split Tuple type arguments.
    fn split_tuple_types(inner: &str) -> Result<Vec<&str>, RowBinaryError> {
        let mut result = Vec::new();
        let mut start = 0;
        let mut depth = 0;

        for (i, c) in inner.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => {
                    result.push(inner[start..i].trim());
                    start = i + 1;
                }
                _ => {}
            }
        }

        if start < inner.len() {
            result.push(inner[start..].trim());
        }

        Ok(result)
    }

    /// Find the top-level comma position for splitting type arguments.
    fn find_top_level_comma(s: &str) -> Result<usize, RowBinaryError> {
        let mut depth = 0;
        for (i, c) in s.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => return Ok(i),
                _ => {}
            }
        }
        Err(RowBinaryError::InvalidTypeSpec {
            spec: s.to_string(),
        })
    }

    /// Parse enum variants from the inner string.
    fn parse_enum_variants<T: std::str::FromStr>(
        inner: &str,
    ) -> Result<Vec<(String, T)>, RowBinaryError>
    where
        T::Err: std::fmt::Debug,
    {
        let mut variants = Vec::new();

        // Simple parsing: split by comma at top level
        for part in Self::split_tuple_types(inner)? {
            // Each part is like 'name' = value
            let eq_pos = part
                .rfind('=')
                .ok_or_else(|| RowBinaryError::InvalidTypeSpec {
                    spec: inner.to_string(),
                })?;
            let name = part[..eq_pos].trim().trim_matches('\'').to_string();
            let value_str = part[eq_pos + 1..].trim();
            let value = value_str
                .parse::<T>()
                .map_err(|_| RowBinaryError::InvalidTypeSpec {
                    spec: inner.to_string(),
                })?;
            variants.push((name, value));
        }

        Ok(variants)
    }

    /// Get the default value for this type.
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
        assert_eq!(ClickHouseType::parse("Int8").unwrap(), ClickHouseType::Int8);
        assert_eq!(
            ClickHouseType::parse("Int64").unwrap(),
            ClickHouseType::Int64
        );
        assert_eq!(
            ClickHouseType::parse("UInt32").unwrap(),
            ClickHouseType::UInt32
        );
        assert_eq!(
            ClickHouseType::parse("Float64").unwrap(),
            ClickHouseType::Float64
        );
        assert_eq!(
            ClickHouseType::parse("String").unwrap(),
            ClickHouseType::String
        );
        assert_eq!(ClickHouseType::parse("Bool").unwrap(), ClickHouseType::Bool);
        assert_eq!(ClickHouseType::parse("Date").unwrap(), ClickHouseType::Date);
        assert_eq!(
            ClickHouseType::parse("DateTime").unwrap(),
            ClickHouseType::DateTime
        );
    }

    #[test]
    fn test_parse_nullable() {
        assert_eq!(
            ClickHouseType::parse("Nullable(String)").unwrap(),
            ClickHouseType::Nullable(Box::new(ClickHouseType::String))
        );
        assert_eq!(
            ClickHouseType::parse("Nullable(Int64)").unwrap(),
            ClickHouseType::Nullable(Box::new(ClickHouseType::Int64))
        );
    }

    #[test]
    fn test_parse_array() {
        assert_eq!(
            ClickHouseType::parse("Array(String)").unwrap(),
            ClickHouseType::Array(Box::new(ClickHouseType::String))
        );
        assert_eq!(
            ClickHouseType::parse("Array(Nullable(Int32))").unwrap(),
            ClickHouseType::Array(Box::new(ClickHouseType::Nullable(Box::new(
                ClickHouseType::Int32
            ))))
        );
    }

    #[test]
    fn test_parse_datetime64() {
        match ClickHouseType::parse("DateTime64(3)").unwrap() {
            ClickHouseType::DateTime64 {
                precision,
                timezone,
            } => {
                assert_eq!(precision, 3);
                assert_eq!(timezone, None);
            }
            _ => panic!("Expected DateTime64"),
        }

        match ClickHouseType::parse("DateTime64(9, 'UTC')").unwrap() {
            ClickHouseType::DateTime64 {
                precision,
                timezone,
            } => {
                assert_eq!(precision, 9);
                assert_eq!(timezone, Some("UTC".to_string()));
            }
            _ => panic!("Expected DateTime64"),
        }
    }

    #[test]
    fn test_parse_fixed_string() {
        assert_eq!(
            ClickHouseType::parse("FixedString(32)").unwrap(),
            ClickHouseType::FixedString(32)
        );
    }

    #[test]
    fn test_parse_low_cardinality() {
        assert_eq!(
            ClickHouseType::parse("LowCardinality(String)").unwrap(),
            ClickHouseType::LowCardinality(Box::new(ClickHouseType::String))
        );
    }
}
