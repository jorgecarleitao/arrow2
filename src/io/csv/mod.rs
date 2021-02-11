//! Transfer data between the Arrow memory format and CSV (comma-separated values).

use crate::error::ArrowError;
use chrono;
use csv;

impl From<csv::Error> for ArrowError {
    fn from(error: csv::Error) -> Self {
        ArrowError::ExternalError(Box::new(error))
    }
}

impl From<chrono::ParseError> for ArrowError {
    fn from(error: chrono::ParseError) -> Self {
        ArrowError::ExternalError(Box::new(error))
    }
}

mod parser;
pub mod reader;
pub mod writer;

mod infer_schema;
mod read_boolean;
mod read_primitive;
pub use infer_schema::{infer_file_schema, infer_schema_from_files};
pub use read_boolean::{new_boolean_array, BooleanParser};
pub use read_primitive::{new_primitive_array, PrimitiveParser};
