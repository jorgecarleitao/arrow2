//! Read and write from and to Apache Avro

pub mod read;

use crate::error::ArrowError;

impl From<avro_rs::SerError> for ArrowError {
    fn from(error: avro_rs::SerError) -> Self {
        ArrowError::External("".to_string(), Box::new(error))
    }
}
