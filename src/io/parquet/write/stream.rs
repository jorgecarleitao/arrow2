use futures::stream::Stream;

use parquet2::write::RowGroupIter;
use parquet2::{
    metadata::{KeyValue, SchemaDescriptor},
    write::stream::write_stream as parquet_write_stream,
    write::stream::write_stream_stream as parquet_write_stream_stream,
};

use crate::datatypes::*;
use crate::error::{ArrowError, Result};

use super::schema::schema_to_metadata_key;
use super::WriteOptions;

/// Writes
pub async fn write_stream<'a, W, I>(
    writer: &mut W,
    row_groups: I,
    schema: &Schema,
    parquet_schema: SchemaDescriptor,
    options: WriteOptions,
    key_value_metadata: Option<Vec<KeyValue>>,
) -> Result<u64>
where
    W: std::io::Write,
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

/// Async writes
pub async fn write_stream_stream<'a, W, I>(
    writer: &mut W,
    row_groups: I,
    schema: &Schema,
    parquet_schema: SchemaDescriptor,
    options: WriteOptions,
    key_value_metadata: Option<Vec<KeyValue>>,
) -> Result<u64>
where
    W: futures::io::AsyncWrite + Unpin + Send,
    I: Stream<Item = Result<RowGroupIter<'a, ArrowError>>>,
{
    let key_value_metadata = key_value_metadata
        .map(|mut x| {
            x.push(schema_to_metadata_key(schema));
            x
        })
        .or_else(|| Some(vec![schema_to_metadata_key(schema)]));

    let created_by = Some("Arrow2 - Native Rust implementation of Arrow".to_string());
    Ok(parquet_write_stream_stream(
        writer,
        row_groups,
        parquet_schema,
        options,
        created_by,
        key_value_metadata,
    )
    .await?)
}
