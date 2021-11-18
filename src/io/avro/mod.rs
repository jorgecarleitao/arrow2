#![deny(missing_docs)]
//! Read and write from and to Apache Avro

pub mod read;
#[cfg(feature = "io_avro_async")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_avro_async")))]
pub mod read_async;

use crate::error::ArrowError;

impl From<avro_rs::Error> for ArrowError {
    fn from(error: avro_rs::Error) -> Self {
        ArrowError::External("".to_string(), Box::new(error))
    }
}
