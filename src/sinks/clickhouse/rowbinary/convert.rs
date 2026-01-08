//! Value conversion utilities for RowBinary serialization.

use std::io::Write;

use chrono::{DateTime, NaiveDate};
use vector_lib::event::Value;

use super::{
    error::RowBinaryError,
    serialize::{write_string, write_varint},
};

/// Convert a Value to i64.
pub(super) fn value_to_i64(value: &Value) -> Result<i64, RowBinaryError> {
    match value {
        Value::Integer(i) => Ok(*i),
        Value::Float(f) => Ok(f.into_inner() as i64),
        Value::Boolean(b) => Ok(if *b { 1 } else { 0 }),
        Value::Bytes(b) => {
            let s = String::from_utf8_lossy(b);
            s.parse().map_err(|_| RowBinaryError::TypeMismatch {
                expected: "Integer".to_string(),
                actual: format!("String: {}", s),
            })
        }
        _ => Err(RowBinaryError::TypeMismatch {
            expected: "Integer".to_string(),
            actual: format!("{:?}", value),
        }),
    }
}

/// Convert a Value to i128.
pub(super) fn value_to_i128(value: &Value) -> Result<i128, RowBinaryError> {
    match value {
        Value::Integer(i) => Ok(*i as i128),
        Value::Float(f) => Ok(f.into_inner() as i128),
        Value::Bytes(b) => {
            let s = String::from_utf8_lossy(b);
            s.parse().map_err(|_| RowBinaryError::TypeMismatch {
                expected: "Integer".to_string(),
                actual: format!("String: {}", s),
            })
        }
        _ => Err(RowBinaryError::TypeMismatch {
            expected: "Integer".to_string(),
            actual: format!("{:?}", value),
        }),
    }
}

/// Convert a Value to u64.
pub(super) fn value_to_u64(value: &Value) -> Result<u64, RowBinaryError> {
    match value {
        Value::Integer(i) => {
            if *i < 0 {
                Err(RowBinaryError::ValueOutOfRange {
                    type_name: "unsigned".to_string(),
                    value: i.to_string(),
                })
            } else {
                Ok(*i as u64)
            }
        }
        Value::Float(f) => {
            let v = f.into_inner();
            if v < 0.0 {
                Err(RowBinaryError::ValueOutOfRange {
                    type_name: "unsigned".to_string(),
                    value: v.to_string(),
                })
            } else {
                Ok(v as u64)
            }
        }
        Value::Boolean(b) => Ok(if *b { 1 } else { 0 }),
        Value::Bytes(b) => {
            let s = String::from_utf8_lossy(b);
            s.parse().map_err(|_| RowBinaryError::TypeMismatch {
                expected: "Unsigned Integer".to_string(),
                actual: format!("String: {}", s),
            })
        }
        _ => Err(RowBinaryError::TypeMismatch {
            expected: "Unsigned Integer".to_string(),
            actual: format!("{:?}", value),
        }),
    }
}

/// Convert a Value to u128.
pub(super) fn value_to_u128(value: &Value) -> Result<u128, RowBinaryError> {
    match value {
        Value::Integer(i) => {
            if *i < 0 {
                Err(RowBinaryError::ValueOutOfRange {
                    type_name: "unsigned".to_string(),
                    value: i.to_string(),
                })
            } else {
                Ok(*i as u128)
            }
        }
        Value::Float(f) => {
            let v = f.into_inner();
            if v < 0.0 {
                Err(RowBinaryError::ValueOutOfRange {
                    type_name: "unsigned".to_string(),
                    value: v.to_string(),
                })
            } else {
                Ok(v as u128)
            }
        }
        Value::Bytes(b) => {
            let s = String::from_utf8_lossy(b);
            s.parse().map_err(|_| RowBinaryError::TypeMismatch {
                expected: "Unsigned Integer".to_string(),
                actual: format!("String: {}", s),
            })
        }
        _ => Err(RowBinaryError::TypeMismatch {
            expected: "Unsigned Integer".to_string(),
            actual: format!("{:?}", value),
        }),
    }
}

/// Convert a Value to f64.
pub(super) fn value_to_f64(value: &Value) -> Result<f64, RowBinaryError> {
    match value {
        Value::Float(f) => Ok(f.into_inner()),
        Value::Integer(i) => Ok(*i as f64),
        Value::Bytes(b) => {
            let s = String::from_utf8_lossy(b);
            s.parse().map_err(|_| RowBinaryError::TypeMismatch {
                expected: "Float".to_string(),
                actual: format!("String: {}", s),
            })
        }
        _ => Err(RowBinaryError::TypeMismatch {
            expected: "Float".to_string(),
            actual: format!("{:?}", value),
        }),
    }
}

/// Convert a Value to bool.
pub(super) fn value_to_bool(value: &Value) -> Result<bool, RowBinaryError> {
    match value {
        Value::Boolean(b) => Ok(*b),
        Value::Integer(i) => Ok(*i != 0),
        Value::Bytes(b) => {
            let s = String::from_utf8_lossy(b).to_lowercase();
            match s.as_str() {
                "true" | "1" | "yes" => Ok(true),
                "false" | "0" | "no" => Ok(false),
                _ => Err(RowBinaryError::TypeMismatch {
                    expected: "Boolean".to_string(),
                    actual: format!("String: {}", s),
                }),
            }
        }
        _ => Err(RowBinaryError::TypeMismatch {
            expected: "Boolean".to_string(),
            actual: format!("{:?}", value),
        }),
    }
}

/// Convert a Value to a String (used for cases where a String is needed, e.g., UUID parsing).
/// For direct writing to a buffer, prefer `write_value_as_string` to avoid allocations.
pub(super) fn value_to_string(value: &Value) -> Result<String, RowBinaryError> {
    match value {
        Value::Bytes(b) => Ok(String::from_utf8_lossy(b).into_owned()),
        Value::Integer(i) => Ok(i.to_string()),
        Value::Float(f) => Ok(f.to_string()),
        Value::Boolean(b) => Ok(if *b { "true" } else { "false" }.to_string()),
        Value::Timestamp(ts) => Ok(ts.to_rfc3339()),
        Value::Null => Ok(String::new()),
        _ => Err(RowBinaryError::TypeMismatch {
            expected: "String".to_string(),
            actual: format!("{:?}", value),
        }),
    }
}

/// Write a Value as a ClickHouse String directly to the writer, avoiding intermediate allocations.
/// This is optimized for the common case of writing string values in RowBinary format.
pub(super) fn write_value_as_string<W: Write>(
    writer: &mut W,
    value: &Value,
) -> Result<(), RowBinaryError> {
    match value {
        Value::Bytes(b) => {
            // Fast path: if bytes are valid UTF-8, write directly without allocation
            match std::str::from_utf8(b) {
                Ok(s) => write_string(writer, s),
                Err(_) => {
                    // Slow path: lossy conversion needed (uncommon)
                    let s = String::from_utf8_lossy(b);
                    write_string(writer, &s)
                }
            }
        }
        Value::Integer(i) => {
            // Use a stack buffer to avoid heap allocation (i64 max is 20 digits + sign)
            let mut buf = [0u8; 21];
            let s = format_i64(*i, &mut buf);
            write_string(writer, s)
        }
        Value::Float(f) => {
            // Use a stack buffer for float formatting
            // f64 in decimal can be up to ~24 chars (including sign, decimal, exponent)
            let mut buf = [0u8; 32];
            let n = {
                use std::io::Write as _;
                let mut cursor = std::io::Cursor::new(&mut buf[..]);
                write!(cursor, "{}", f.into_inner()).unwrap();
                cursor.position() as usize
            };
            let s = std::str::from_utf8(&buf[..n]).unwrap();
            write_string(writer, s)
        }
        Value::Boolean(b) => {
            // Write static strings directly - no allocation
            if *b {
                write_string(writer, "true")
            } else {
                write_string(writer, "false")
            }
        }
        Value::Timestamp(ts) => {
            // RFC3339 format still needs allocation, but this is less common
            let s = ts.to_rfc3339();
            write_string(writer, &s)
        }
        Value::Null => {
            // Empty string: just write length 0
            write_varint(writer, 0)?;
            Ok(())
        }
        _ => Err(RowBinaryError::TypeMismatch {
            expected: "String".to_string(),
            actual: format!("{:?}", value),
        }),
    }
}

/// Format an i64 into a stack buffer, returning the string slice.
/// This avoids heap allocation for integer-to-string conversion.
#[inline]
fn format_i64(mut n: i64, buf: &mut [u8; 21]) -> &str {
    let is_negative = n < 0;
    let mut i = buf.len();

    if n == 0 {
        buf[buf.len() - 1] = b'0';
        return std::str::from_utf8(&buf[buf.len() - 1..]).unwrap();
    }

    // Handle negative numbers
    if is_negative {
        n = n.wrapping_neg();
    }

    // Write digits from right to left
    while n > 0 {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }

    // Add negative sign
    if is_negative {
        i -= 1;
        buf[i] = b'-';
    }

    std::str::from_utf8(&buf[i..]).unwrap()
}

/// Convert a Value to an array reference.
pub(super) fn value_to_array(value: &Value) -> Result<&Vec<Value>, RowBinaryError> {
    match value {
        Value::Array(arr) => Ok(arr),
        _ => Err(RowBinaryError::TypeMismatch {
            expected: "Array".to_string(),
            actual: format!("{:?}", value),
        }),
    }
}

/// Convert a Value to date days (days since 1970-01-01).
pub(super) fn value_to_date_days(value: &Value) -> Result<i32, RowBinaryError> {
    match value {
        Value::Timestamp(ts) => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let date = ts.date_naive();
            Ok(date.signed_duration_since(epoch).num_days() as i32)
        }
        Value::Integer(i) => Ok(*i as i32),
        Value::Bytes(b) => {
            let s = String::from_utf8_lossy(b);
            // Try to parse as date string
            if let Ok(date) = NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                Ok(date.signed_duration_since(epoch).num_days() as i32)
            } else {
                // Try to parse as integer
                s.parse().map_err(|_| RowBinaryError::TypeMismatch {
                    expected: "Date".to_string(),
                    actual: format!("String: {}", s),
                })
            }
        }
        _ => Err(RowBinaryError::TypeMismatch {
            expected: "Date".to_string(),
            actual: format!("{:?}", value),
        }),
    }
}

/// Convert a Value to Unix timestamp (seconds since epoch).
pub(super) fn value_to_unix_timestamp(value: &Value) -> Result<i64, RowBinaryError> {
    match value {
        Value::Timestamp(ts) => Ok(ts.timestamp()),
        Value::Integer(i) => Ok(*i),
        Value::Float(f) => Ok(f.into_inner() as i64),
        Value::Bytes(b) => {
            let s = String::from_utf8_lossy(b);
            // Try to parse as RFC3339 timestamp
            if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
                Ok(dt.timestamp())
            } else if let Ok(ts) = s.parse::<i64>() {
                // Try as Unix timestamp
                Ok(ts)
            } else {
                Err(RowBinaryError::TypeMismatch {
                    expected: "DateTime".to_string(),
                    actual: format!("String: {}", s),
                })
            }
        }
        _ => Err(RowBinaryError::TypeMismatch {
            expected: "DateTime".to_string(),
            actual: format!("{:?}", value),
        }),
    }
}

/// Convert a Value to timestamp nanoseconds.
pub(super) fn value_to_timestamp_nanos(value: &Value) -> Result<i64, RowBinaryError> {
    match value {
        Value::Timestamp(ts) => {
            // timestamp_nanos() can overflow for dates far from epoch
            // Use a safer calculation
            let secs = ts.timestamp();
            let nanos = ts.timestamp_subsec_nanos() as i64;
            Ok(secs.saturating_mul(1_000_000_000).saturating_add(nanos))
        }
        Value::Integer(i) => {
            // Assume nanoseconds
            Ok(*i)
        }
        Value::Float(f) => {
            // Assume seconds with fractional part
            let secs = f.into_inner();
            Ok((secs * 1_000_000_000.0) as i64)
        }
        Value::Bytes(b) => {
            let s = String::from_utf8_lossy(b);
            // Try to parse as RFC3339 timestamp
            if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
                let secs = dt.timestamp();
                let nanos = dt.timestamp_subsec_nanos() as i64;
                Ok(secs.saturating_mul(1_000_000_000).saturating_add(nanos))
            } else if let Ok(ts) = s.parse::<i64>() {
                // Assume nanoseconds
                Ok(ts)
            } else {
                Err(RowBinaryError::TypeMismatch {
                    expected: "DateTime64".to_string(),
                    actual: format!("String: {}", s),
                })
            }
        }
        _ => Err(RowBinaryError::TypeMismatch {
            expected: "DateTime64".to_string(),
            actual: format!("{:?}", value),
        }),
    }
}
