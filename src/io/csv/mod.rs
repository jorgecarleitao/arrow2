//! Transfer data between the Arrow memory format and CSV (comma-separated values).

use crate::error::ArrowError;

impl From<csv::Error> for ArrowError {
    fn from(error: csv::Error) -> Self {
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

mod parser;
mod reader;
mod writer;

mod infer_schema;
mod read_boolean;
mod read_primitive;
mod read_utf8;
pub use infer_schema::infer_schema;
pub use parser::DefaultParser;
pub use read_boolean::{new_boolean_array, BooleanParser};
pub use read_primitive::{new_primitive_array, PrimitiveParser};
pub use read_utf8::{new_utf8_array, Utf8Parser};

pub use reader::*;
pub use writer::{Writer, WriterBuilder};
