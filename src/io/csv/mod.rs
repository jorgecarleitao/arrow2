//! Transfer data between the Arrow memory format and CSV (comma-separated values).

use crate::error::ArrowError;

pub use csv::Error as CSVError;

impl From<CSVError> for ArrowError {
    fn from(error: CSVError) -> Self {
        ArrowError::External("".to_string(), Box::new(error))
    }
}

impl From<chrono::ParseError> for ArrowError {
    fn from(error: chrono::ParseError) -> Self {
        ArrowError::External("".to_string(), Box::new(error))
    }
}

impl From<std::str::Utf8Error> for ArrowError {
    fn from(error: std::str::Utf8Error) -> Self {
        ArrowError::External("".to_string(), Box::new(error))
    }
}

pub mod read;
pub mod write;
