use crate::datatypes::Schema;
use crate::error::Result;

mod convert;
mod metadata;

pub use convert::parquet_to_arrow_schema;
pub use metadata::read_schema_from_metadata;
pub use parquet2::metadata::{FileMetaData, KeyValue, SchemaDescriptor};

pub fn get_schema(metadata: &FileMetaData) -> Result<Schema> {
    let schema = read_schema_from_metadata(metadata.key_value_metadata())?;
    Ok(schema).transpose().unwrap_or_else(|| {
        parquet_to_arrow_schema(metadata.schema(), metadata.key_value_metadata())
    })
}
