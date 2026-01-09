//! Common ClickHouse type parsing utilities.
//!
//! This module provides the canonical `ClickHouseType` representation and parsing,
//! used by both Arrow and RowBinary format implementations.

/// Decimal precision constants for ClickHouse decimal types.
pub mod precision {
    pub const DECIMAL32: u8 = 9;
    pub const DECIMAL64: u8 = 18;
    pub const DECIMAL128: u8 = 38;
    pub const DECIMAL256: u8 = 76;
}

/// Errors that can occur during type parsing.
#[derive(Debug, Clone, PartialEq)]
pub enum TypeParseError {
    /// Malformed arguments in type specification.
    MalformedArguments { input: String },
    /// Invalid type specification.
    InvalidSpec { spec: String },
    /// Unsupported type.
    UnsupportedType { type_name: String },
}

impl std::fmt::Display for TypeParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TypeParseError::MalformedArguments { input } => {
                write!(f, "Expected parentheses around arguments in '{}'", input)
            }
            TypeParseError::InvalidSpec { spec } => {
                write!(f, "Invalid type specification: {}", spec)
            }
            TypeParseError::UnsupportedType { type_name } => {
                write!(f, "Unsupported ClickHouse type: {}", type_name)
            }
        }
    }
}

impl std::error::Error for TypeParseError {}

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
    pub fn parse(type_str: &str) -> Result<Self, TypeParseError> {
        let type_str = type_str.trim();

        // Handle wrapper types
        if let Some(inner) = strip_wrapper(type_str, "Nullable") {
            return Ok(ClickHouseType::Nullable(Box::new(Self::parse(inner)?)));
        }
        if let Some(inner) = strip_wrapper(type_str, "LowCardinality") {
            return Ok(ClickHouseType::LowCardinality(Box::new(Self::parse(
                inner,
            )?)));
        }
        if let Some(inner) = strip_wrapper(type_str, "Array") {
            return Ok(ClickHouseType::Array(Box::new(Self::parse(inner)?)));
        }

        // Handle Map type
        if let Some(inner) = strip_wrapper(type_str, "Map") {
            let args = split_type_args(inner, type_str)?;
            if args.len() != 2 {
                return Err(TypeParseError::InvalidSpec {
                    spec: type_str.to_string(),
                });
            }
            return Ok(ClickHouseType::Map {
                key: Box::new(Self::parse(args[0])?),
                value: Box::new(Self::parse(args[1])?),
            });
        }

        // Handle Tuple type
        if let Some(inner) = strip_wrapper(type_str, "Tuple") {
            let args = split_type_args(inner, type_str)?;
            let parsed: Result<Vec<_>, _> = args.iter().map(|t| Self::parse(t)).collect();
            return Ok(ClickHouseType::Tuple(parsed?));
        }

        // Handle FixedString
        if let Some(len_str) = strip_wrapper(type_str, "FixedString") {
            let len = len_str.parse().map_err(|_| TypeParseError::InvalidSpec {
                spec: type_str.to_string(),
            })?;
            return Ok(ClickHouseType::FixedString(len));
        }

        // Handle DateTime64
        if let Some(inner) = strip_wrapper(type_str, "DateTime64") {
            let args = split_type_args(inner, type_str)?;
            let precision = args
                .first()
                .ok_or_else(|| TypeParseError::InvalidSpec {
                    spec: type_str.to_string(),
                })?
                .parse()
                .map_err(|_| TypeParseError::InvalidSpec {
                    spec: type_str.to_string(),
                })?;
            let timezone = args.get(1).map(|s| s.trim_matches('\'').to_string());
            return Ok(ClickHouseType::DateTime64 {
                precision,
                timezone,
            });
        }

        // Handle DateTime with timezone (treat as basic DateTime)
        if strip_wrapper(type_str, "DateTime").is_some() {
            return Ok(ClickHouseType::DateTime);
        }

        // Handle Decimal types
        if let Some(inner) = strip_wrapper(type_str, "Decimal") {
            let args = split_type_args(inner, type_str)?;
            if args.len() != 2 {
                return Err(TypeParseError::InvalidSpec {
                    spec: type_str.to_string(),
                });
            }
            let precision = args[0].parse().map_err(|_| TypeParseError::InvalidSpec {
                spec: type_str.to_string(),
            })?;
            let scale = args[1].parse().map_err(|_| TypeParseError::InvalidSpec {
                spec: type_str.to_string(),
            })?;
            return Ok(ClickHouseType::Decimal { precision, scale });
        }

        for (prefix, ctor) in [
            (
                "Decimal32",
                ClickHouseType::decimal32 as fn(u8) -> ClickHouseType,
            ),
            (
                "Decimal64",
                ClickHouseType::decimal64 as fn(u8) -> ClickHouseType,
            ),
            (
                "Decimal128",
                ClickHouseType::decimal128 as fn(u8) -> ClickHouseType,
            ),
            (
                "Decimal256",
                ClickHouseType::decimal256 as fn(u8) -> ClickHouseType,
            ),
        ] {
            if let Some(scale_str) = strip_wrapper(type_str, prefix) {
                let scale = scale_str
                    .trim()
                    .parse()
                    .map_err(|_| TypeParseError::InvalidSpec {
                        spec: type_str.to_string(),
                    })?;
                return Ok(ctor(scale));
            }
        }

        // Handle Enum types
        if let Some(inner) = strip_wrapper(type_str, "Enum8") {
            return Ok(ClickHouseType::Enum8(parse_enum_variants(inner, type_str)?));
        }
        if let Some(inner) = strip_wrapper(type_str, "Enum16") {
            return Ok(ClickHouseType::Enum16(parse_enum_variants(
                inner, type_str,
            )?));
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
            _ => Err(TypeParseError::UnsupportedType {
                type_name: type_str.to_string(),
            }),
        }
    }

    /// Returns true if this type is nullable.
    pub const fn is_nullable(&self) -> bool {
        matches!(self, ClickHouseType::Nullable(_))
    }

    /// Unwraps Nullable/LowCardinality wrappers, returning the base type and nullable flag.
    pub fn unwrap_modifiers(&self) -> (&ClickHouseType, bool) {
        match self {
            ClickHouseType::Nullable(inner) => {
                let (base, _) = inner.unwrap_modifiers();
                (base, true)
            }
            ClickHouseType::LowCardinality(inner) => inner.unwrap_modifiers(),
            _ => (self, false),
        }
    }
}

// Helper function constructors for Decimal variants (needed for the loop above)
impl ClickHouseType {
    const fn decimal32(scale: u8) -> Self {
        ClickHouseType::Decimal32 { scale }
    }
    const fn decimal64(scale: u8) -> Self {
        ClickHouseType::Decimal64 { scale }
    }
    const fn decimal128(scale: u8) -> Self {
        ClickHouseType::Decimal128 { scale }
    }
    const fn decimal256(scale: u8) -> Self {
        ClickHouseType::Decimal256 { scale }
    }
}

/// Strips a wrapper type from a type string.
pub fn strip_wrapper<'a>(ty: &'a str, wrapper_name: &str) -> Option<&'a str> {
    ty.strip_prefix(wrapper_name)?
        .trim_start()
        .strip_prefix('(')?
        .strip_suffix(')')
}

/// Extracts a type name from the start of a string.
pub fn extract_type_name(input: &str) -> (&str, &str) {
    for (i, c) in input.char_indices() {
        if c.is_alphabetic() || c == '_' || (i > 0 && c.is_numeric()) {
            continue;
        }
        return (&input[..i], &input[i..]);
    }
    (input, "")
}

/// Parses comma-separated arguments from a parenthesized string.
pub fn parse_type_args(input: &str) -> Result<Vec<&str>, TypeParseError> {
    let trimmed = input.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return Err(TypeParseError::MalformedArguments {
            input: input.to_string(),
        });
    }
    let inner = &trimmed[1..trimmed.len() - 1];
    if inner.trim().is_empty() {
        return Ok(vec![]);
    }
    split_at_top_level_commas(inner)
}

/// Splits a string at top-level commas.
pub fn split_at_top_level_commas(input: &str) -> Result<Vec<&str>, TypeParseError> {
    let mut args = Vec::new();
    let mut start = 0;
    let mut depth = 0;
    let mut in_quotes = false;

    for (i, c) in input.char_indices() {
        match c {
            '\'' if !in_quotes => in_quotes = true,
            '\'' if in_quotes => in_quotes = false,
            '(' if !in_quotes => depth += 1,
            ')' if !in_quotes => depth -= 1,
            ',' if depth == 0 && !in_quotes => {
                args.push(input[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = input[start..].trim();
    if !last.is_empty() {
        args.push(last);
    }
    Ok(args)
}

/// Unwraps type modifiers from a type string (for backward compatibility).
pub fn unwrap_modifiers(ch_type: &str) -> (&str, bool) {
    let mut current = ch_type.trim();
    let mut is_nullable = false;

    loop {
        if let Some(inner) = strip_wrapper(current, "Nullable") {
            is_nullable = true;
            current = inner.trim();
        } else if let Some(inner) = strip_wrapper(current, "LowCardinality") {
            current = inner.trim();
        } else {
            break;
        }
    }
    (current, is_nullable)
}

fn split_type_args<'a>(inner: &'a str, type_str: &str) -> Result<Vec<&'a str>, TypeParseError> {
    split_at_top_level_commas(inner).map_err(|_| TypeParseError::InvalidSpec {
        spec: type_str.to_string(),
    })
}

fn parse_enum_variants<T: std::str::FromStr>(
    inner: &str,
    type_str: &str,
) -> Result<Vec<(String, T)>, TypeParseError>
where
    T::Err: std::fmt::Debug,
{
    let mut variants = Vec::new();
    for part in split_type_args(inner, type_str)? {
        let eq_pos = part.rfind('=').ok_or_else(|| TypeParseError::InvalidSpec {
            spec: type_str.to_string(),
        })?;
        let name = part[..eq_pos].trim().trim_matches('\'').to_string();
        let value = part[eq_pos + 1..]
            .trim()
            .parse()
            .map_err(|_| TypeParseError::InvalidSpec {
                spec: type_str.to_string(),
            })?;
        variants.push((name, value));
    }
    Ok(variants)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_wrapper() {
        assert_eq!(
            strip_wrapper("Nullable(String)", "Nullable"),
            Some("String")
        );
        assert_eq!(
            strip_wrapper("LowCardinality(String)", "LowCardinality"),
            Some("String")
        );
        assert_eq!(strip_wrapper("String", "Nullable"), None);
    }

    #[test]
    fn test_extract_type_name() {
        assert_eq!(extract_type_name("Decimal(10, 2)"), ("Decimal", "(10, 2)"));
        assert_eq!(extract_type_name("Int32"), ("Int32", ""));
    }

    #[test]
    fn test_parse_type_args() {
        assert_eq!(parse_type_args("(10, 2)").unwrap(), vec!["10", "2"]);
        assert_eq!(parse_type_args("(3, 'UTC')").unwrap(), vec!["3", "'UTC'"]);
    }

    #[test]
    fn test_unwrap_modifiers() {
        assert_eq!(unwrap_modifiers("String"), ("String", false));
        assert_eq!(unwrap_modifiers("Nullable(String)"), ("String", true));
        assert_eq!(
            unwrap_modifiers("LowCardinality(Nullable(String))"),
            ("String", true)
        );
    }

    #[test]
    fn test_parse_simple_types() {
        assert_eq!(
            ClickHouseType::parse("Int64").unwrap(),
            ClickHouseType::Int64
        );
        assert_eq!(
            ClickHouseType::parse("String").unwrap(),
            ClickHouseType::String
        );
        assert_eq!(ClickHouseType::parse("Bool").unwrap(), ClickHouseType::Bool);
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
    }

    #[test]
    fn test_parse_decimal() {
        match ClickHouseType::parse("Decimal(18, 6)").unwrap() {
            ClickHouseType::Decimal { precision, scale } => {
                assert_eq!(precision, 18);
                assert_eq!(scale, 6);
            }
            _ => panic!("Expected Decimal"),
        }
    }
}
