//! Arrow type conversion for ClickHouse types.

use arrow::datatypes::{DataType, TimeUnit};

use crate::sinks::clickhouse::type_parser::{ClickHouseType, precision};

/// Converts a ClickHouse type string to an Arrow DataType.
/// Returns a tuple of (DataType, is_nullable).
pub fn clickhouse_type_to_arrow(ch_type: &str) -> Result<(DataType, bool), String> {
    let parsed = ClickHouseType::parse(ch_type).map_err(|e| e.to_string())?;
    let (base, is_nullable) = parsed.unwrap_modifiers();
    let data_type = base.to_arrow_type()?;
    Ok((data_type, is_nullable))
}

impl ClickHouseType {
    /// Convert this ClickHouseType to an Arrow DataType.
    pub fn to_arrow_type(&self) -> Result<DataType, String> {
        match self {
            // Integers
            ClickHouseType::Int8 => Ok(DataType::Int8),
            ClickHouseType::Int16 => Ok(DataType::Int16),
            ClickHouseType::Int32 => Ok(DataType::Int32),
            ClickHouseType::Int64 => Ok(DataType::Int64),
            ClickHouseType::UInt8 => Ok(DataType::UInt8),
            ClickHouseType::UInt16 => Ok(DataType::UInt16),
            ClickHouseType::UInt32 => Ok(DataType::UInt32),
            ClickHouseType::UInt64 => Ok(DataType::UInt64),

            // Floats
            ClickHouseType::Float32 => Ok(DataType::Float32),
            ClickHouseType::Float64 => Ok(DataType::Float64),

            // Bool
            ClickHouseType::Bool => Ok(DataType::Boolean),

            // Strings
            ClickHouseType::String | ClickHouseType::FixedString(_) => Ok(DataType::Utf8),

            // Dates
            ClickHouseType::Date | ClickHouseType::Date32 => Ok(DataType::Date32),
            ClickHouseType::DateTime => Ok(DataType::Timestamp(TimeUnit::Second, None)),
            ClickHouseType::DateTime64 { precision, .. } => {
                let unit = match precision {
                    0 => TimeUnit::Second,
                    1..=3 => TimeUnit::Millisecond,
                    4..=6 => TimeUnit::Microsecond,
                    7..=9 => TimeUnit::Nanosecond,
                    _ => return Err(format!("Unsupported DateTime64 precision: {}", precision)),
                };
                Ok(DataType::Timestamp(unit, None))
            }

            // Decimals
            ClickHouseType::Decimal { precision: p, scale } => {
                Ok(if *p <= precision::DECIMAL128 {
                    DataType::Decimal128(*p, *scale as i8)
                } else {
                    DataType::Decimal256(*p, *scale as i8)
                })
            }
            ClickHouseType::Decimal32 { scale } => Ok(DataType::Decimal128(precision::DECIMAL32, *scale as i8)),
            ClickHouseType::Decimal64 { scale } => Ok(DataType::Decimal128(precision::DECIMAL64, *scale as i8)),
            ClickHouseType::Decimal128 { scale } => Ok(DataType::Decimal128(precision::DECIMAL128, *scale as i8)),
            ClickHouseType::Decimal256 { scale } => Ok(DataType::Decimal256(precision::DECIMAL256, *scale as i8)),

            // Unsupported complex types
            ClickHouseType::Array(_) => Err("Array type is not supported for Arrow conversion".to_string()),
            ClickHouseType::Tuple(_) => Err("Tuple type is not supported for Arrow conversion".to_string()),
            ClickHouseType::Map { .. } => Err("Map type is not supported for Arrow conversion".to_string()),

            // Wrappers (should be unwrapped before calling)
            ClickHouseType::Nullable(inner) => inner.to_arrow_type(),
            ClickHouseType::LowCardinality(inner) => inner.to_arrow_type(),

            // Other unsupported types
            _ => Err(format!("Type {:?} is not supported for Arrow conversion", self)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clickhouse_type_mapping() {
        assert_eq!(clickhouse_type_to_arrow("String").unwrap(), (DataType::Utf8, false));
        assert_eq!(clickhouse_type_to_arrow("Int64").unwrap(), (DataType::Int64, false));
        assert_eq!(clickhouse_type_to_arrow("Bool").unwrap(), (DataType::Boolean, false));
    }

    #[test]
    fn test_datetime64_precision_mapping() {
        assert_eq!(clickhouse_type_to_arrow("DateTime64(0)").unwrap(), (DataType::Timestamp(TimeUnit::Second, None), false));
        assert_eq!(clickhouse_type_to_arrow("DateTime64(3)").unwrap(), (DataType::Timestamp(TimeUnit::Millisecond, None), false));
        assert_eq!(clickhouse_type_to_arrow("DateTime64(6)").unwrap(), (DataType::Timestamp(TimeUnit::Microsecond, None), false));
        assert_eq!(clickhouse_type_to_arrow("DateTime64(9)").unwrap(), (DataType::Timestamp(TimeUnit::Nanosecond, None), false));
    }

    #[test]
    fn test_nullable_type_mapping() {
        assert_eq!(clickhouse_type_to_arrow("Nullable(String)").unwrap(), (DataType::Utf8, true));
        assert_eq!(clickhouse_type_to_arrow("Nullable(Int64)").unwrap(), (DataType::Int64, true));
    }

    #[test]
    fn test_lowcardinality_type_mapping() {
        assert_eq!(clickhouse_type_to_arrow("LowCardinality(String)").unwrap(), (DataType::Utf8, false));
        assert_eq!(clickhouse_type_to_arrow("LowCardinality(Nullable(String))").unwrap(), (DataType::Utf8, true));
    }

    #[test]
    fn test_decimal_type_mapping() {
        assert_eq!(clickhouse_type_to_arrow("Decimal(10, 2)").unwrap(), (DataType::Decimal128(10, 2), false));
        assert_eq!(clickhouse_type_to_arrow("Decimal32(4)").unwrap(), (DataType::Decimal128(9, 4), false));
        assert_eq!(clickhouse_type_to_arrow("Decimal256(20)").unwrap(), (DataType::Decimal256(76, 20), false));
    }

    #[test]
    fn test_array_type_not_supported() {
        assert!(clickhouse_type_to_arrow("Array(Int32)").is_err());
    }

    #[test]
    fn test_map_type_not_supported() {
        assert!(clickhouse_type_to_arrow("Map(String, Int64)").is_err());
    }
}
