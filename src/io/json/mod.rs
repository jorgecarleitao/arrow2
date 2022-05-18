//! Convert data between the Arrow memory format and JSON line-delimited records.

pub mod read;
pub mod write;

use crate::error::Error;

impl From<serde_json::error::Error> for Error {
    fn from(error: serde_json::error::Error) -> Self {
        Error::External("".to_string(), Box::new(error))
    }
}
