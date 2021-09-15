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

#[cfg(feature = "read_csv")]
pub mod read;

#[cfg(feature = "write_csv")]
pub mod write;
