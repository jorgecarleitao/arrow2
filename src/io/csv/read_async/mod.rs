//! Asynchronous reading of CSV

// Re-export for usage by consumers.
pub use csv_async::{AsyncReader, AsyncReaderBuilder, ByteRecord};

mod infer_schema;

//pub use deserialize::{deserialize_batch, deserialize_column};
pub use super::utils::infer;
pub use infer_schema::infer_schema;
//pub use reader::*;

pub use csv_async::Error as CSVError;

impl From<CSVError> for crate::error::ArrowError {
    fn from(error: CSVError) -> Self {
        crate::error::ArrowError::External("".to_string(), Box::new(error))
    }
}
