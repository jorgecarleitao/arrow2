use futures::stream::Stream;

use parquet2::{
    metadata::SchemaDescriptor, schema::KeyValue,
    write::stream::write_stream as parquet_write_stream,
};
use parquet2::{write::RowGroupIter, write::WriteOptions};

use crate::datatypes::*;
use crate::error::{ArrowError, Result};

use super::schema::schema_to_metadata_key;

/// Writes
pub async fn write_stream<'a, W, I>(
    writer: &mut W,
    row_groups: I,
    schema: &Schema,
    parquet_schema: SchemaDescriptor,
    options: WriteOptions,
    key_value_metadata: Option<Vec<KeyValue>>,
) -> Result<()>
where
    W: std::io::Write + std::io::Seek,
    I: Stream<Item = Result<RowGroupIter<'a, ArrowError>>>,
{
    let key_value_metadata = key_value_metadata
        .map(|mut x| {
            x.push(schema_to_metadata_key(schema));
            x
        })
        .or_else(|| Some(vec![schema_to_metadata_key(schema)]));

    let created_by = Some("Arrow2 - Native Rust implementation of Arrow".to_string());
    Ok(parquet_write_stream(
        writer,
        row_groups,
        parquet_schema,
        options,
        created_by,
        key_value_metadata,
    )
    .await?)
}
