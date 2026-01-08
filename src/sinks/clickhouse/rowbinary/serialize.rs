//! Serialization functions for RowBinary format.

use std::io::Write;

use bytes::Bytes;
use vector_lib::event::Value;

use super::{
    convert::{
        value_to_array, value_to_date_days, value_to_string, value_to_timestamp_nanos,
        value_to_unix_timestamp, write_value_as_string,
    },
    error::RowBinaryError,
    types::ClickHouseType,
};

/// Write a varint-encoded unsigned integer to the buffer.
///
/// This is LEB128 encoding as used by ClickHouse for string lengths.
pub fn write_varint<W: Write>(writer: &mut W, mut value: u64) -> Result<(), RowBinaryError> {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            writer.write_all(&[byte])?;
            break;
        } else {
            writer.write_all(&[byte | 0x80])?;
        }
    }
    Ok(())
}

/// Write a string in ClickHouse RowBinary format (varint length + bytes).
pub fn write_string<W: Write>(writer: &mut W, s: &str) -> Result<(), RowBinaryError> {
    let bytes = s.as_bytes();
    write_varint(writer, bytes.len() as u64)?;
    writer.write_all(bytes)?;
    Ok(())
}

/// Write bytes in ClickHouse RowBinary format (varint length + bytes).
pub fn write_bytes<W: Write>(writer: &mut W, bytes: &[u8]) -> Result<(), RowBinaryError> {
    write_varint(writer, bytes.len() as u64)?;
    writer.write_all(bytes)?;
    Ok(())
}

/// Helper function to serialize Decimal values.
fn serialize_decimal<W: Write>(
    writer: &mut W,
    value: &Value,
    precision: u8,
    scale: u8,
) -> Result<(), RowBinaryError> {
    use super::convert::value_to_f64;

    let byte_size = if precision <= 9 {
        4
    } else if precision <= 18 {
        8
    } else if precision <= 38 {
        16
    } else {
        32
    };

    let v = value_to_f64(value)?;
    let scale_factor = 10f64.powi(scale as i32);
    let scaled = (v * scale_factor).round() as i128;

    match byte_size {
        4 => {
            let v = i32::try_from(scaled).map_err(|_| RowBinaryError::ValueOutOfRange {
                type_name: format!("Decimal({}, {})", precision, scale),
                value: v.to_string(),
            })?;
            writer.write_all(&v.to_le_bytes())?;
        }
        8 => {
            let v = i64::try_from(scaled).map_err(|_| RowBinaryError::ValueOutOfRange {
                type_name: format!("Decimal({}, {})", precision, scale),
                value: v.to_string(),
            })?;
            writer.write_all(&v.to_le_bytes())?;
        }
        16 => {
            writer.write_all(&scaled.to_le_bytes())?;
        }
        32 => {
            // For 256-bit, extend to 32 bytes
            writer.write_all(&scaled.to_le_bytes())?;
            let sign_ext = if scaled < 0 { -1i128 } else { 0i128 };
            writer.write_all(&sign_ext.to_le_bytes())?;
        }
        _ => unreachable!(),
    }
    Ok(())
}

/// Serialize a Vector Value to ClickHouse RowBinary format.
pub fn serialize_value<W: Write>(
    writer: &mut W,
    value: &Value,
    ch_type: &ClickHouseType,
) -> Result<(), RowBinaryError> {
    use super::convert::{value_to_bool, value_to_i64, value_to_i128, value_to_u64, value_to_u128};

    match ch_type {
        ClickHouseType::Nullable(inner) => {
            if matches!(value, Value::Null) {
                // Write 1 byte for NULL flag
                writer.write_all(&[1])?;
            } else {
                // Write 0 byte for non-NULL, then the actual value
                writer.write_all(&[0])?;
                serialize_value(writer, value, inner)?;
            }
        }
        ClickHouseType::LowCardinality(inner) => {
            // LowCardinality is transparent in RowBinary - serialize the inner type directly
            serialize_value(writer, value, inner)?;
        }
        ClickHouseType::Int8 => {
            let v = value_to_i64(value)?;
            let v = i8::try_from(v).map_err(|_| RowBinaryError::ValueOutOfRange {
                type_name: "Int8".to_string(),
                value: v.to_string(),
            })?;
            writer.write_all(&v.to_le_bytes())?;
        }
        ClickHouseType::Int16 => {
            let v = value_to_i64(value)?;
            let v = i16::try_from(v).map_err(|_| RowBinaryError::ValueOutOfRange {
                type_name: "Int16".to_string(),
                value: v.to_string(),
            })?;
            writer.write_all(&v.to_le_bytes())?;
        }
        ClickHouseType::Int32 => {
            let v = value_to_i64(value)?;
            let v = i32::try_from(v).map_err(|_| RowBinaryError::ValueOutOfRange {
                type_name: "Int32".to_string(),
                value: v.to_string(),
            })?;
            writer.write_all(&v.to_le_bytes())?;
        }
        ClickHouseType::Int64 => {
            let v = value_to_i64(value)?;
            writer.write_all(&v.to_le_bytes())?;
        }
        ClickHouseType::Int128 => {
            let v = value_to_i128(value)?;
            writer.write_all(&v.to_le_bytes())?;
        }
        ClickHouseType::Int256 => {
            // Int256 is stored as two i128s in little-endian order
            let v = value_to_i128(value)?;
            let (lo, hi) = if v >= 0 { (v, 0i128) } else { (v, -1i128) };
            writer.write_all(&lo.to_le_bytes())?;
            writer.write_all(&hi.to_le_bytes())?;
        }
        ClickHouseType::UInt8 => {
            let v = value_to_u64(value)?;
            let v = u8::try_from(v).map_err(|_| RowBinaryError::ValueOutOfRange {
                type_name: "UInt8".to_string(),
                value: v.to_string(),
            })?;
            writer.write_all(&v.to_le_bytes())?;
        }
        ClickHouseType::UInt16 => {
            let v = value_to_u64(value)?;
            let v = u16::try_from(v).map_err(|_| RowBinaryError::ValueOutOfRange {
                type_name: "UInt16".to_string(),
                value: v.to_string(),
            })?;
            writer.write_all(&v.to_le_bytes())?;
        }
        ClickHouseType::UInt32 => {
            let v = value_to_u64(value)?;
            let v = u32::try_from(v).map_err(|_| RowBinaryError::ValueOutOfRange {
                type_name: "UInt32".to_string(),
                value: v.to_string(),
            })?;
            writer.write_all(&v.to_le_bytes())?;
        }
        ClickHouseType::UInt64 => {
            let v = value_to_u64(value)?;
            writer.write_all(&v.to_le_bytes())?;
        }
        ClickHouseType::UInt128 => {
            let v = value_to_u128(value)?;
            writer.write_all(&v.to_le_bytes())?;
        }
        ClickHouseType::UInt256 => {
            // UInt256 is stored as two u128s in little-endian order
            let v = value_to_u128(value)?;
            writer.write_all(&v.to_le_bytes())?;
            writer.write_all(&0u128.to_le_bytes())?;
        }
        ClickHouseType::Float32 => {
            use super::convert::value_to_f64;
            let v = value_to_f64(value)?;
            writer.write_all(&(v as f32).to_le_bytes())?;
        }
        ClickHouseType::Float64 => {
            use super::convert::value_to_f64;
            let v = value_to_f64(value)?;
            writer.write_all(&v.to_le_bytes())?;
        }
        ClickHouseType::Bool => {
            let v = value_to_bool(value)?;
            writer.write_all(&[if v { 1 } else { 0 }])?;
        }
        ClickHouseType::String => {
            // Use optimized direct-write function to avoid intermediate String allocation
            write_value_as_string(writer, value)?;
        }
        ClickHouseType::FixedString(len) => {
            // Optimized path: get bytes directly when possible to avoid allocation
            let bytes: std::borrow::Cow<'_, [u8]> = match value {
                Value::Bytes(b) => std::borrow::Cow::Borrowed(b.as_ref()),
                Value::Null => std::borrow::Cow::Borrowed(&[]),
                _ => {
                    // Fall back to string conversion for other types
                    let s = value_to_string(value)?;
                    std::borrow::Cow::Owned(s.into_bytes())
                }
            };
            if bytes.len() > *len {
                return Err(RowBinaryError::ValueOutOfRange {
                    type_name: format!("FixedString({})", len),
                    value: format!("string of length {}", bytes.len()),
                });
            }
            // Write the bytes, padded with zeros to the fixed length
            writer.write_all(&bytes)?;
            // Optimize: write all padding zeros at once if needed
            let padding = *len - bytes.len();
            if padding > 0 {
                static ZEROS: [u8; 256] = [0u8; 256];
                let mut remaining = padding;
                while remaining > 0 {
                    let chunk = remaining.min(ZEROS.len());
                    writer.write_all(&ZEROS[..chunk])?;
                    remaining -= chunk;
                }
            }
        }
        ClickHouseType::Date => {
            // Date is stored as UInt16 - days since 1970-01-01
            let days = value_to_date_days(value)?;
            let days = u16::try_from(days).map_err(|_| RowBinaryError::ValueOutOfRange {
                type_name: "Date".to_string(),
                value: days.to_string(),
            })?;
            writer.write_all(&days.to_le_bytes())?;
        }
        ClickHouseType::Date32 => {
            // Date32 is stored as Int32 - days since 1970-01-01
            let days = value_to_date_days(value)?;
            writer.write_all(&days.to_le_bytes())?;
        }
        ClickHouseType::DateTime => {
            // DateTime is stored as UInt32 - Unix timestamp
            let ts = value_to_unix_timestamp(value)?;
            let ts = u32::try_from(ts).map_err(|_| RowBinaryError::ValueOutOfRange {
                type_name: "DateTime".to_string(),
                value: ts.to_string(),
            })?;
            writer.write_all(&ts.to_le_bytes())?;
        }
        ClickHouseType::DateTime64 { precision, .. } => {
            // DateTime64 is stored as Int64 - value depends on precision
            let ts_nanos = value_to_timestamp_nanos(value)?;
            let divisor = match precision {
                0 => 1_000_000_000i64, // seconds
                1 => 100_000_000i64,   // deciseconds
                2 => 10_000_000i64,    // centiseconds
                3 => 1_000_000i64,     // milliseconds
                4 => 100_000i64,
                5 => 10_000i64,
                6 => 1_000i64, // microseconds
                7 => 100i64,
                8 => 10i64,
                9 => 1i64, // nanoseconds
                _ => {
                    return Err(RowBinaryError::InvalidTypeSpec {
                        spec: format!("DateTime64({})", precision),
                    });
                }
            };
            let value = ts_nanos / divisor;
            writer.write_all(&value.to_le_bytes())?;
        }
        ClickHouseType::UUID => {
            // UUID is stored as two UInt64s
            let uuid_str = value_to_string(value)?;
            let uuid = uuid::Uuid::parse_str(&uuid_str).map_err(|_| {
                RowBinaryError::SerializationError {
                    message: format!("Invalid UUID: {}", uuid_str),
                }
            })?;
            let bytes = uuid.as_bytes();
            // ClickHouse stores UUID as two big-endian u64s
            // First 8 bytes, then last 8 bytes
            writer.write_all(&bytes[..8])?;
            writer.write_all(&bytes[8..])?;
        }
        ClickHouseType::IPv4 => {
            // IPv4 is stored as UInt32 in big-endian byte order
            let ip_str = value_to_string(value)?;
            let ip: std::net::Ipv4Addr =
                ip_str
                    .parse()
                    .map_err(|_| RowBinaryError::SerializationError {
                        message: format!("Invalid IPv4 address: {}", ip_str),
                    })?;
            writer.write_all(&ip.octets())?;
        }
        ClickHouseType::IPv6 => {
            // IPv6 is stored as 16 bytes
            let ip_str = value_to_string(value)?;
            let ip: std::net::Ipv6Addr =
                ip_str
                    .parse()
                    .map_err(|_| RowBinaryError::SerializationError {
                        message: format!("Invalid IPv6 address: {}", ip_str),
                    })?;
            writer.write_all(&ip.octets())?;
        }
        ClickHouseType::Decimal { precision, scale } => {
            serialize_decimal(writer, value, *precision, *scale)?;
        }
        ClickHouseType::Decimal32 { scale } => {
            serialize_decimal(writer, value, 9, *scale)?;
        }
        ClickHouseType::Decimal64 { scale } => {
            serialize_decimal(writer, value, 18, *scale)?;
        }
        ClickHouseType::Decimal128 { scale } => {
            serialize_decimal(writer, value, 38, *scale)?;
        }
        ClickHouseType::Decimal256 { scale } => {
            serialize_decimal(writer, value, 76, *scale)?;
        }
        ClickHouseType::Array(inner) => {
            let arr = value_to_array(value)?;
            // Write array length as varint
            write_varint(writer, arr.len() as u64)?;
            // Write each element
            for item in arr {
                serialize_value(writer, item, inner)?;
            }
        }
        ClickHouseType::Map {
            key,
            value: val_type,
        } => {
            match value {
                Value::Object(map) => {
                    // Write map size as varint
                    write_varint(writer, map.len() as u64)?;
                    // Write each key-value pair
                    for (k, v) in map {
                        serialize_value(writer, &Value::Bytes(Bytes::from(k.to_string())), key)?;
                        serialize_value(writer, v, val_type)?;
                    }
                }
                Value::Array(arr) => {
                    // Array of tuples
                    write_varint(writer, arr.len() as u64)?;
                    for item in arr {
                        if let Value::Array(pair) = item {
                            if pair.len() == 2 {
                                serialize_value(writer, &pair[0], key)?;
                                serialize_value(writer, &pair[1], val_type)?;
                            } else {
                                return Err(RowBinaryError::TypeMismatch {
                                    expected: "Array of 2-element tuples".to_string(),
                                    actual: format!("Array of {}-element tuples", pair.len()),
                                });
                            }
                        } else {
                            return Err(RowBinaryError::TypeMismatch {
                                expected: "Array of tuples".to_string(),
                                actual: format!("{:?}", item),
                            });
                        }
                    }
                }
                _ => {
                    return Err(RowBinaryError::TypeMismatch {
                        expected: "Map or Array".to_string(),
                        actual: format!("{:?}", value),
                    });
                }
            }
        }
        ClickHouseType::Tuple(types) => {
            let arr = value_to_array(value)?;
            if arr.len() != types.len() {
                return Err(RowBinaryError::TypeMismatch {
                    expected: format!("Tuple with {} elements", types.len()),
                    actual: format!("Array with {} elements", arr.len()),
                });
            }
            for (item, item_type) in arr.iter().zip(types.iter()) {
                serialize_value(writer, item, item_type)?;
            }
        }
        ClickHouseType::Enum8(variants) => {
            let value_str = value_to_string(value)?;
            let enum_value = variants
                .iter()
                .find(|(name, _)| name == &value_str)
                .map(|(_, v)| *v)
                .ok_or_else(|| RowBinaryError::SerializationError {
                    message: format!("Unknown enum variant: {}", value_str),
                })?;
            writer.write_all(&enum_value.to_le_bytes())?;
        }
        ClickHouseType::Enum16(variants) => {
            let value_str = value_to_string(value)?;
            let enum_value = variants
                .iter()
                .find(|(name, _)| name == &value_str)
                .map(|(_, v)| *v)
                .ok_or_else(|| RowBinaryError::SerializationError {
                    message: format!("Unknown enum variant: {}", value_str),
                })?;
            writer.write_all(&enum_value.to_le_bytes())?;
        }
        ClickHouseType::JSON => {
            // JSON is serialized as a String containing JSON
            let json_str = match value {
                Value::Object(map) => {
                    serde_json::to_string(map).map_err(|e| RowBinaryError::SerializationError {
                        message: format!("Failed to serialize JSON: {}", e),
                    })?
                }
                Value::Array(arr) => {
                    serde_json::to_string(arr).map_err(|e| RowBinaryError::SerializationError {
                        message: format!("Failed to serialize JSON: {}", e),
                    })?
                }
                _ => value_to_string(value)?,
            };
            write_string(writer, &json_str)?;
        }
    }
    Ok(())
}
