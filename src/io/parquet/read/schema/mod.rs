//! APIs to handle Parquet <-> Arrow schemas.
use crate::datatypes::Schema;
use crate::error::Result;

mod convert;
mod metadata;

pub use metadata::read_schema_from_metadata;
pub use parquet2::metadata::{FileMetaData, KeyValue, SchemaDescriptor};
pub use parquet2::schema::types::ParquetType;

pub(crate) use convert::*;

use self::metadata::parse_key_value_metadata;

/// Infers a [`Schema`] from parquet's [`FileMetaData`]. This first looks for the metadata key
/// `"ARROW:schema"`; if it does not exist, it converts the parquet types declared in the
/// file's parquet schema to Arrow's equivalent.
/// # Error
/// This function errors iff the key `"ARROW:schema"` exists but is not correctly encoded,
/// indicating that that the file's arrow metadata was incorrectly written.
pub fn infer_schema(file_metadata: &FileMetaData) -> Result<Schema> {
    let mut metadata = parse_key_value_metadata(file_metadata.key_value_metadata());

    let schema = read_schema_from_metadata(&mut metadata)?;
    Ok(schema.unwrap_or_else(|| {
        let fields = parquet_to_arrow_schema(file_metadata.schema().fields());
        Schema { fields, metadata }
    }))
}
