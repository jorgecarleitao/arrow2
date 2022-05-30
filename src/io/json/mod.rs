//! Convert data between the Arrow memory format and JSON line-delimited records.

pub mod read;
pub mod write;

use crate::error::Error;

impl From<json_deserializer::Error> for Error {
    fn from(error: json_deserializer::Error) -> Self {
        Error::ExternalFormat(error.to_string())
    }
}
