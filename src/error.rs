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
    /// Error associated with incompatible schemas.
    Schema(String),
    /// Errors associated with IO
    Io(std::io::Error),
    /// When an invalid argument is passed to a function.
    InvalidArgumentError(String),
    /// Error during import or export to/from C Data Interface
    Ffi(String),
    /// Error during import or export to/from IPC
    Ipc(String),
    /// Error during import or export to/from a format
    ExternalFormat(String),
    /// Whenever pushing to a container fails because it does not support more entries.
    /// (e.g. maximum size of the keys of a dictionary overflowed)
    KeyOverflowError,
    /// Error during arithmetic operation. Normally returned
    /// during checked operations
    ArithmeticError(String),
    /// Any other error.
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

impl From<std::str::Utf8Error> for ArrowError {
    fn from(error: std::str::Utf8Error) -> Self {
        ArrowError::External("".to_string(), Box::new(error))
    }
}

impl From<simdutf8::basic::Utf8Error> for ArrowError {
    fn from(error: simdutf8::basic::Utf8Error) -> Self {
        ArrowError::External("".to_string(), Box::new(error))
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
            ArrowError::KeyOverflowError => {
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

/// Typedef for a [`std::result::Result`] of an [`ArrowError`].
pub type Result<T> = std::result::Result<T, ArrowError>;
