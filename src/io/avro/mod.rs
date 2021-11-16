#![deny(missing_docs)]
//! Read and write from and to Apache Avro

pub mod read;

use crate::error::ArrowError;

impl From<avro_rs::Error> for ArrowError {
    fn from(error: avro_rs::Error) -> Self {
        ArrowError::External("".to_string(), Box::new(error))
    }
}
