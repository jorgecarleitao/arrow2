//! APIs to read from and write to Parquet format.
use crate::error::ArrowError;

pub mod read;
pub mod write;

const ARROW_SCHEMA_META_KEY: &str = "ARROW:schema";

impl From<parquet2::error::ParquetError> for ArrowError {
    fn from(error: parquet2::error::ParquetError) -> Self {
        ArrowError::External("".to_string(), Box::new(error))
    }
}

impl From<ArrowError> for parquet2::error::ParquetError {
    fn from(error: ArrowError) -> Self {
        parquet2::error::ParquetError::General(error.to_string())
    }
}
