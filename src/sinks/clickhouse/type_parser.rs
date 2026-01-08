//! Common ClickHouse type parsing utilities.
//!
//! This module provides shared functionality for parsing ClickHouse type strings,
//! used by both Arrow and RowBinary format implementations.

/// Decimal precision constants for ClickHouse decimal types.
pub mod precision {
    pub const DECIMAL32: u8 = 9;
    pub const DECIMAL64: u8 = 18;
    pub const DECIMAL128: u8 = 38;
    pub const DECIMAL256: u8 = 76;
}

/// Strips a wrapper type (like `Nullable`, `LowCardinality`, `Array`) from a type string.
///
/// Returns the inner content if the type matches the wrapper pattern.
///
/// # Example
/// ```ignore
/// assert_eq!(strip_wrapper("Nullable(String)", "Nullable"), Some("String"));
/// assert_eq!(strip_wrapper("String", "Nullable"), None);
/// ```
pub fn strip_wrapper<'a>(ty: &'a str, wrapper_name: &str) -> Option<&'a str> {
    ty.strip_prefix(wrapper_name)?
        .trim_start()
        .strip_prefix('(')?
        .strip_suffix(')')
}

/// Extracts an identifier (type name) from the start of a string.
///
/// Returns a tuple of (identifier, remaining_string).
///
/// # Example
/// ```ignore
/// assert_eq!(extract_type_name("Decimal(10, 2)"), ("Decimal", "(10, 2)"));
/// assert_eq!(extract_type_name("DateTime64(3)"), ("DateTime64", "(3)"));
/// assert_eq!(extract_type_name("Int32"), ("Int32", ""));
/// ```
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
///
/// Handles nested parentheses and quoted strings correctly.
///
/// # Example
/// ```ignore
/// assert_eq!(parse_type_args("(10, 2)"), Ok(vec!["10", "2"]));
/// assert_eq!(parse_type_args("(3, 'UTC')"), Ok(vec!["3", "'UTC'"]));
/// assert_eq!(parse_type_args("(Nullable(String))"), Ok(vec!["Nullable(String)"]));
/// ```
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

/// Splits a string at top-level commas (not inside parentheses or quotes).
///
/// This is useful for parsing type arguments like Map(K, V) or Tuple(A, B, C).
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

    // Add the last argument
    let last = input[start..].trim();
    if !last.is_empty() {
        args.push(last);
    }

    Ok(args)
}

/// Unwraps ClickHouse type modifiers like `Nullable()` and `LowCardinality()`.
///
/// Returns a tuple of (base_type, is_nullable).
///
/// # Example
/// ```ignore
/// assert_eq!(unwrap_modifiers("LowCardinality(Nullable(String))"), ("String", true));
/// assert_eq!(unwrap_modifiers("Nullable(Int64)"), ("Int64", true));
/// assert_eq!(unwrap_modifiers("String"), ("String", false));
/// ```
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_wrapper() {
        assert_eq!(strip_wrapper("Nullable(String)", "Nullable"), Some("String"));
        assert_eq!(strip_wrapper("Nullable(Int64)", "Nullable"), Some("Int64"));
        assert_eq!(
            strip_wrapper("LowCardinality(String)", "LowCardinality"),
            Some("String")
        );
        assert_eq!(
            strip_wrapper("LowCardinality(Nullable(String))", "LowCardinality"),
            Some("Nullable(String)")
        );
        assert_eq!(strip_wrapper("String", "Nullable"), None);
        assert_eq!(strip_wrapper("Array(Int32)", "Nullable"), None);
    }

    #[test]
    fn test_extract_type_name() {
        assert_eq!(extract_type_name("Decimal(10, 2)"), ("Decimal", "(10, 2)"));
        assert_eq!(extract_type_name("DateTime64(3)"), ("DateTime64", "(3)"));
        assert_eq!(extract_type_name("Int32"), ("Int32", ""));
        assert_eq!(
            extract_type_name("LowCardinality(String)"),
            ("LowCardinality", "(String)")
        );
        assert_eq!(extract_type_name("Decimal128(10)"), ("Decimal128", "(10)"));
    }

    #[test]
    fn test_parse_type_args() {
        // Simple cases
        assert_eq!(
            parse_type_args("(10, 2)").unwrap(),
            vec!["10", "2"]
        );
        assert_eq!(parse_type_args("(3)").unwrap(), vec!["3"]);
        assert_eq!(parse_type_args("()").unwrap(), Vec::<&str>::new());

        // With spaces
        assert_eq!(
            parse_type_args("( 10 , 2 )").unwrap(),
            vec!["10", "2"]
        );

        // With nested parentheses
        assert_eq!(
            parse_type_args("(Nullable(String))").unwrap(),
            vec!["Nullable(String)"]
        );
        assert_eq!(
            parse_type_args("(Array(Int32), String)").unwrap(),
            vec!["Array(Int32)", "String"]
        );

        // With quotes
        assert_eq!(
            parse_type_args("(3, 'UTC')").unwrap(),
            vec!["3", "'UTC'"]
        );
        assert_eq!(
            parse_type_args("(9, 'America/New_York')").unwrap(),
            vec!["9", "'America/New_York'"]
        );

        // Complex nested case
        assert_eq!(
            parse_type_args("(Tuple(Int32, String), Array(Float64))").unwrap(),
            vec!["Tuple(Int32, String)", "Array(Float64)"]
        );

        // Error cases
        assert!(parse_type_args("10, 2").is_err()); // Missing parentheses
        assert!(parse_type_args("(10, 2").is_err()); // Missing closing paren
    }

    #[test]
    fn test_unwrap_modifiers() {
        assert_eq!(unwrap_modifiers("String"), ("String", false));
        assert_eq!(unwrap_modifiers("Int64"), ("Int64", false));
        assert_eq!(unwrap_modifiers("Nullable(String)"), ("String", true));
        assert_eq!(unwrap_modifiers("Nullable(Int64)"), ("Int64", true));
        assert_eq!(
            unwrap_modifiers("LowCardinality(String)"),
            ("String", false)
        );
        assert_eq!(
            unwrap_modifiers("LowCardinality(Nullable(String))"),
            ("String", true)
        );
        assert_eq!(
            unwrap_modifiers("Nullable(LowCardinality(String))"),
            ("String", true)
        );
        assert_eq!(
            unwrap_modifiers("DateTime64(3)"),
            ("DateTime64(3)", false)
        );
    }

    #[test]
    fn test_split_at_top_level_commas() {
        assert_eq!(
            split_at_top_level_commas("Int32, String").unwrap(),
            vec!["Int32", "String"]
        );
        assert_eq!(
            split_at_top_level_commas("Array(Int32), String").unwrap(),
            vec!["Array(Int32)", "String"]
        );
        assert_eq!(
            split_at_top_level_commas("'name' = 1, 'other' = 2").unwrap(),
            vec!["'name' = 1", "'other' = 2"]
        );
    }
}

