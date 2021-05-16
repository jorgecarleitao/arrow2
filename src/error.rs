//! Defines [`ArrowError`], representing all errors returned by this crate.
use std::fmt::{Debug, Display, Formatter};

use std::error::Error;

/// Enum with all errors in this crate.
#[derive(Debug)]
pub enum ArrowError {
    /// Returned when functionality is not yet available.
    NotYetImplemented(String),
    /// Triggered by an external error, such as CSV, serde, chrono.
    External(String, Box<dyn Error + Send + Sync>),
    Schema(String),
    Io(std::io::Error),
    InvalidArgumentError(String),
    /// Error during import or export to/from C Data Interface
    Ffi(String),
    /// Error during import or export to/from IPC
    Ipc(String),
    /// Error during import or export to/from a format
    ExternalFormat(String),
    DictionaryKeyOverflowError,
    /// Error during arithmetic operation. Normally returned
    /// during checked operations
    ArithmeticError(String),
    Other(String),
}

impl ArrowError {
    /// Wraps an external error in an `ArrowError`.
    pub fn from_external_error(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::External("".to_string(), Box::new(error))
    }
}

impl From<::std::io::Error> for ArrowError {
    fn from(error: std::io::Error) -> Self {
        ArrowError::Io(error)
    }
}

impl Display for ArrowError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrowError::NotYetImplemented(source) => {
                write!(f, "Not yet implemented: {}", &source)
            }
            ArrowError::External(message, source) => {
                write!(f, "External error{}: {}", message, &source)
            }
            ArrowError::Schema(desc) => write!(f, "Schema error: {}", desc),
            ArrowError::Io(desc) => write!(f, "Io error: {}", desc),
            ArrowError::InvalidArgumentError(desc) => {
                write!(f, "Invalid argument error: {}", desc)
            }
            ArrowError::Ffi(desc) => {
                write!(f, "FFI error: {}", desc)
            }
            ArrowError::Ipc(desc) => {
                write!(f, "IPC error: {}", desc)
            }
            ArrowError::ExternalFormat(desc) => {
                write!(f, "External format error: {}", desc)
            }
            ArrowError::DictionaryKeyOverflowError => {
                write!(f, "Dictionary key bigger than the key type")
            }
            ArrowError::ArithmeticError(desc) => {
                write!(f, "Arithmetic error: {}", desc)
            }
            ArrowError::Other(message) => {
                write!(f, "{}", message)
            }
        }
    }
}

impl Error for ArrowError {}

pub type Result<T> = std::result::Result<T, ArrowError>;
