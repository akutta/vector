//! ClickHouse type parsing and conversion to Arrow types.

use arrow::datatypes::{DataType, TimeUnit};

use crate::sinks::clickhouse::type_parser::{
    extract_type_name, parse_type_args, precision, unwrap_modifiers,
};

fn unsupported(ch_type: &str, kind: &str) -> String {
    format!(
        "{kind} type '{ch_type}' is not supported. \
         ClickHouse {kind} types cannot be automatically converted to Arrow format."
    )
}

/// Converts a ClickHouse type string to an Arrow DataType.
/// Returns a tuple of (DataType, is_nullable).
pub fn clickhouse_type_to_arrow(ch_type: &str) -> Result<(DataType, bool), String> {
    let (base_type, is_nullable) = unwrap_modifiers(ch_type);
    let (type_name, _) = extract_type_name(base_type);

    let data_type = match type_name {
        // Numeric
        "Int8" => DataType::Int8,
        "Int16" => DataType::Int16,
        "Int32" => DataType::Int32,
        "Int64" => DataType::Int64,
        "UInt8" => DataType::UInt8,
        "UInt16" => DataType::UInt16,
        "UInt32" => DataType::UInt32,
        "UInt64" => DataType::UInt64,
        "Float32" => DataType::Float32,
        "Float64" => DataType::Float64,
        "Bool" => DataType::Boolean,
        "Decimal" | "Decimal32" | "Decimal64" | "Decimal128" | "Decimal256" => {
            parse_decimal_type(base_type)?
        }

        // Strings
        "String" | "FixedString" => DataType::Utf8,

        // Date and time types (timezones not currently handled, defaults to UTC)
        "Date" | "Date32" => DataType::Date32,
        "DateTime" => DataType::Timestamp(TimeUnit::Second, None),
        "DateTime64" => parse_datetime64_precision(base_type)?,

        // Unsupported
        "Array" => return Err(unsupported(ch_type, "Array")),
        "Tuple" => return Err(unsupported(ch_type, "Tuple")),
        "Map" => return Err(unsupported(ch_type, "Map")),

        // Unknown
        _ => {
            return Err(format!(
                "Unknown ClickHouse type '{}'. This type cannot be automatically converted.",
                type_name
            ));
        }
    };

    Ok((data_type, is_nullable))
}


/// Parses ClickHouse Decimal types and returns the appropriate Arrow decimal type.
/// ClickHouse formats:
/// - Decimal(P, S) -> generic decimal with precision P and scale S
/// - Decimal32(S) -> precision up to 9, scale S
/// - Decimal64(S) -> precision up to 18, scale S
/// - Decimal128(S) -> precision up to 38, scale S
/// - Decimal256(S) -> precision up to 76, scale S
///
/// Uses metadata from ClickHouse's system.columns when available, otherwise falls back to parsing the type string.
fn parse_decimal_type(ch_type: &str) -> Result<DataType, String> {
    // Parse from type string
    let (type_name, args_str) = extract_type_name(ch_type);

    let result = parse_type_args(args_str).ok().and_then(|args| match type_name {
        "Decimal" if args.len() == 2 => args[0].parse::<u8>().ok().zip(args[1].parse::<i8>().ok()),
        "Decimal32" | "Decimal64" | "Decimal128" | "Decimal256" if args.len() == 1 => {
            args[0].parse::<i8>().ok().map(|scale| {
                let prec = match type_name {
                    "Decimal32" => precision::DECIMAL32,
                    "Decimal64" => precision::DECIMAL64,
                    "Decimal128" => precision::DECIMAL128,
                    "Decimal256" => precision::DECIMAL256,
                    _ => unreachable!(),
                };
                (prec, scale)
            })
        }
        _ => None,
    });

    result
        .map(|(prec, scale)| {
            if prec <= precision::DECIMAL128 {
                DataType::Decimal128(prec, scale)
            } else {
                DataType::Decimal256(prec, scale)
            }
        })
        .ok_or_else(|| format!("Could not parse Decimal type '{}'.", ch_type))
}

/// Parses DateTime64 precision and returns the appropriate Arrow timestamp type.
/// DateTime64(0) -> Second
/// DateTime64(3) -> Millisecond
/// DateTime64(6) -> Microsecond
/// DateTime64(9) -> Nanosecond
///
fn parse_datetime64_precision(ch_type: &str) -> Result<DataType, String> {
    // Parse from type string
    let (_type_name, args_str) = extract_type_name(ch_type);

    let args = parse_type_args(args_str).map_err(|e| {
        format!(
            "Could not parse DateTime64 arguments from '{}': {}. Expected format: DateTime64(0-9) or DateTime64(0-9, 'timezone')",
            ch_type, e
        )
    })?;

    // DateTime64(precision) or DateTime64(precision, 'timezone')
    if args.is_empty() {
        return Err(format!(
            "DateTime64 type '{}' has no precision argument. Expected format: DateTime64(0-9) or DateTime64(0-9, 'timezone')",
            ch_type
        ));
    }

    // Parse the precision (first argument)
    match args[0].parse::<u8>() {
        Ok(0) => Ok(DataType::Timestamp(TimeUnit::Second, None)),
        Ok(1..=3) => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
        Ok(4..=6) => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        Ok(7..=9) => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        _ => Err(format!(
            "Unsupported DateTime64 precision in '{}'. Precision must be 0-9",
            ch_type
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function for tests that don't need metadata
    fn convert_type_no_metadata(ch_type: &str) -> Result<(DataType, bool), String> {
        clickhouse_type_to_arrow(ch_type)
    }

    #[test]
    fn test_clickhouse_type_mapping() {
        assert_eq!(
            convert_type_no_metadata("String").expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Utf8, false)
        );
        assert_eq!(
            convert_type_no_metadata("Int64").expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Int64, false)
        );
        assert_eq!(
            convert_type_no_metadata("Float64")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Float64, false)
        );
        assert_eq!(
            convert_type_no_metadata("Bool").expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Boolean, false)
        );
        assert_eq!(
            convert_type_no_metadata("DateTime")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Second, None), false)
        );
    }

    #[test]
    fn test_datetime64_precision_mapping() {
        assert_eq!(
            convert_type_no_metadata("DateTime64(0)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Second, None), false)
        );
        assert_eq!(
            convert_type_no_metadata("DateTime64(3)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Millisecond, None), false)
        );
        assert_eq!(
            convert_type_no_metadata("DateTime64(6)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Microsecond, None), false)
        );
        assert_eq!(
            convert_type_no_metadata("DateTime64(9)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Nanosecond, None), false)
        );
        // Test with timezones
        assert_eq!(
            convert_type_no_metadata("DateTime64(9, 'UTC')")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Nanosecond, None), false)
        );
        assert_eq!(
            convert_type_no_metadata("DateTime64(6, 'UTC')")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Microsecond, None), false)
        );
        assert_eq!(
            convert_type_no_metadata("DateTime64(9, 'America/New_York')")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Nanosecond, None), false)
        );
        // Test edge cases for precision ranges
        assert_eq!(
            convert_type_no_metadata("DateTime64(1)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Millisecond, None), false)
        );
        assert_eq!(
            convert_type_no_metadata("DateTime64(4)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Microsecond, None), false)
        );
        assert_eq!(
            convert_type_no_metadata("DateTime64(7)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Timestamp(TimeUnit::Nanosecond, None), false)
        );
    }

    #[test]
    fn test_nullable_type_mapping() {
        // Non-nullable types
        assert_eq!(
            convert_type_no_metadata("String").expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Utf8, false)
        );
        assert_eq!(
            convert_type_no_metadata("Int64").expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Int64, false)
        );

        // Nullable types
        assert_eq!(
            convert_type_no_metadata("Nullable(String)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Utf8, true)
        );
        assert_eq!(
            convert_type_no_metadata("Nullable(Int64)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Int64, true)
        );
        assert_eq!(
            convert_type_no_metadata("Nullable(Float64)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Float64, true)
        );
    }

    #[test]
    fn test_lowcardinality_type_mapping() {
        assert_eq!(
            convert_type_no_metadata("LowCardinality(String)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Utf8, false)
        );
        assert_eq!(
            convert_type_no_metadata("LowCardinality(FixedString(10))")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Utf8, false)
        );
        // Nullable + LowCardinality
        assert_eq!(
            convert_type_no_metadata("LowCardinality(Nullable(String))")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Utf8, true)
        );
    }

    #[test]
    fn test_decimal_type_mapping() {
        // Generic Decimal(P, S)
        assert_eq!(
            convert_type_no_metadata("Decimal(10, 2)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(10, 2), false)
        );
        assert_eq!(
            convert_type_no_metadata("Decimal(38, 6)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(38, 6), false)
        );
        assert_eq!(
            convert_type_no_metadata("Decimal(50, 10)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal256(50, 10), false)
        );

        // Generic Decimal without spaces and with spaces
        assert_eq!(
            convert_type_no_metadata("Decimal(10,2)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(10, 2), false)
        );
        assert_eq!(
            convert_type_no_metadata("Decimal( 18 , 6 )")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(18, 6), false)
        );

        // Decimal32(S) - precision up to 9
        assert_eq!(
            convert_type_no_metadata("Decimal32(2)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(9, 2), false)
        );
        assert_eq!(
            convert_type_no_metadata("Decimal32(4)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(9, 4), false)
        );

        // Decimal64(S) - precision up to 18
        assert_eq!(
            convert_type_no_metadata("Decimal64(4)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(18, 4), false)
        );
        assert_eq!(
            convert_type_no_metadata("Decimal64(8)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(18, 8), false)
        );

        // Decimal128(S) - precision up to 38
        assert_eq!(
            convert_type_no_metadata("Decimal128(10)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(38, 10), false)
        );

        // Decimal256(S) - precision up to 76
        assert_eq!(
            convert_type_no_metadata("Decimal256(20)")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal256(76, 20), false)
        );

        // With Nullable wrapper
        assert_eq!(
            convert_type_no_metadata("Nullable(Decimal(18, 6))")
                .expect("Failed to convert ClickHouse type to Arrow"),
            (DataType::Decimal128(18, 6), true)
        );
    }

    // Tests for extract_type_name and parse_type_args are in the type_parser module

    #[test]
    fn test_array_type_not_supported() {
        // Array types should return an error
        let result = convert_type_no_metadata("Array(Int32)");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Array type"));
        assert!(err.contains("not supported"));
    }

    #[test]
    fn test_tuple_type_not_supported() {
        // Tuple types should return an error
        let result = convert_type_no_metadata("Tuple(String, Int64)");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Tuple type"));
        assert!(err.contains("not supported"));
    }

    #[test]
    fn test_map_type_not_supported() {
        // Map types should return an error
        let result = convert_type_no_metadata("Map(String, Int64)");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Map type"));
        assert!(err.contains("not supported"));
    }

    #[test]
    fn test_unknown_type_fails() {
        // Unknown types should return an error
        let result = convert_type_no_metadata("UnknownType");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Unknown ClickHouse type"));
    }

    // Tests for unwrap_modifiers are in the type_parser module
}
