#![deny(missing_docs)]
//! Convert data between the Arrow memory format and JSON line-delimited records.

mod read;
mod write;

pub use read::*;
pub use write::*;

use crate::error::ArrowError;

impl From<serde_json::error::Error> for ArrowError {
    fn from(error: serde_json::error::Error) -> Self {
        ArrowError::External("".to_string(), Box::new(error))
    }
}
