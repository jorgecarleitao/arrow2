#![deny(missing_docs)]
#![forbid(unsafe_code)]
//! Convert data between the Arrow memory format and JSON line-delimited records.

pub mod read;
mod write;

pub use write::*;

use crate::error::ArrowError;

impl From<serde_json::error::Error> for ArrowError {
    fn from(error: serde_json::error::Error) -> Self {
        ArrowError::External("".to_string(), Box::new(error))
    }
}
