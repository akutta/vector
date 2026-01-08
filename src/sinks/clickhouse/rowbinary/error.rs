//! Error types for RowBinary serialization.

use snafu::Snafu;

/// Errors that can occur during RowBinary serialization.
#[derive(Debug, Snafu)]
pub enum RowBinaryError {
    #[snafu(display("Failed to serialize value: {}", message))]
    SerializationError { message: String },

    #[snafu(display("Unsupported ClickHouse type: {}", type_name))]
    UnsupportedType { type_name: String },

    #[snafu(display("Type mismatch: expected {}, got {}", expected, actual))]
    TypeMismatch { expected: String, actual: String },

    #[snafu(display("Missing required field: {}", field))]
    MissingField { field: String },

    #[snafu(display("Invalid type specification: {}", spec))]
    InvalidTypeSpec { spec: String },

    #[snafu(display("Value out of range for type {}: {}", type_name, value))]
    ValueOutOfRange { type_name: String, value: String },

    #[snafu(display("IO error: {}", source))]
    IoError { source: std::io::Error },
}

impl From<std::io::Error> for RowBinaryError {
    fn from(source: std::io::Error) -> Self {
        RowBinaryError::IoError { source }
    }
}
