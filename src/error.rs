//! Defines [`ArrowError`], representing all errors returned by this crate.
use std::fmt::{Debug, Display, Formatter};

use std::error::Error;

/// Enum with all errors in this crate.
#[derive(Debug)]
#[non_exhaustive]
pub enum ArrowError {
    /// Returned when functionality is not yet available.
    NotYetImplemented(String),
    /// Wrapper for an error triggered by a dependency
    External(String, Box<dyn Error + Send + Sync>),
    /// Wrapper for IO errors
    Io(std::io::Error),
    /// When an invalid argument is passed to a function.
    InvalidArgumentError(String),
    /// Error during import or export to/from a format
    ExternalFormat(String),
    /// Whenever pushing to a container fails because it does not support more entries.
    /// The solution is usually to use a higher-capacity container-backing type.
    Overflow,
    /// Whenever incoming data from the C data interface, IPC or Flight does not fulfil the Arrow specification.
    OutOfSpec(String),
    /// Various error sources.
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
            ArrowError::Io(desc) => write!(f, "Io error: {}", desc),
            ArrowError::InvalidArgumentError(desc) => {
                write!(f, "Invalid argument error: {}", desc)
            }
            ArrowError::ExternalFormat(desc) => {
                write!(f, "External format error: {}", desc)
            }
            ArrowError::Overflow => {
                write!(f, "Operation overflew the backing container.")
            }
            ArrowError::OutOfSpec(message) => {
                write!(f, "{}", message)
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
