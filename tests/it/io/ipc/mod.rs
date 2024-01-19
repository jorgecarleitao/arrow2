mod common;
mod read;
mod write;

pub use common::read_gzip_json;

#[cfg(feature = "io_ipc_write_async")]
mod write_stream_async;

#[cfg(feature = "io_ipc_write_async")]
mod write_file_async;

#[cfg(feature = "io_ipc_read_async")]
mod read_stream_async;

#[cfg(feature = "io_ipc_read_async")]
mod read_file_async;

mod mmap;
use std::io::Cursor;
use std::sync::Arc;

use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{Schema, SchemaRef, Field};
use arrow2::error::*;
use arrow2::io::ipc::read::{read_file_metadata, FileReader};
use arrow2::io::ipc::write::*;
use arrow2::io::ipc::IpcField;

pub(crate) fn write(
    batches: &[Chunk<Box<dyn Array>>],
    schema: &SchemaRef,
    ipc_fields: Option<Vec<IpcField>>,
    compression: Option<Compression>,
) -> Result<Vec<u8>> {
    let result = vec![];
    let options = WriteOptions { compression };
    let mut writer = FileWriter::try_new(result, schema.clone(), ipc_fields.clone(), options)?;
    for batch in batches {
        writer.write(batch, ipc_fields.as_ref().map(|x| x.as_ref()))?;
    }
    writer.finish()?;
    Ok(writer.into_inner())
}

fn round_trip(
    columns: Chunk<Box<dyn Array>>,
    schema: SchemaRef,
    ipc_fields: Option<Vec<IpcField>>,
    compression: Option<Compression>,
) -> Result<()> {
    let (expected_schema, expected_batches) = (schema.clone(), vec![columns]);

    let result = write(&expected_batches, &schema, ipc_fields, compression)?;
    let mut reader = Cursor::new(result);
    let metadata = read_file_metadata(&mut reader)?;
    let schema = metadata.schema.clone();

    let reader = FileReader::new(reader, metadata, None, None);

    assert_eq!(schema, expected_schema);

    let batches = reader.collect::<Result<Vec<_>>>()?;

    assert_eq!(batches, expected_batches);
    Ok(())
}

fn prep_schema(array: &dyn Array) -> SchemaRef {
    let fields = vec![Field::new("a", array.data_type().clone(), true)];
    Arc::new(Schema::from(fields))
}

#[test]
fn write_boolean() -> Result<()> {
    let array = BooleanArray::from([Some(true), Some(false), None, Some(true)]).boxed();
    let schema = prep_schema(array.as_ref());
    let columns = Chunk::try_new(vec![array])?;
    round_trip(columns, schema, None, Some(Compression::ZSTD))
}

#[test]
fn write_sliced_utf8() -> Result<()> {
    let array = Utf8Array::<i32>::from_slice(["aa", "bb"])
        .sliced(1, 1)
        .boxed();
    let schema = prep_schema(array.as_ref());
    let columns = Chunk::try_new(vec![array])?;
    round_trip(columns, schema, None, Some(Compression::ZSTD))
}

#[test]
fn write_binview() -> Result<()> {
    let array = Utf8ViewArray::from_slice([Some("foo"), Some("bar"), None, Some("hamlet")]).boxed();
    let schema = prep_schema(array.as_ref());
    let columns = Chunk::try_new(vec![array])?;
    round_trip(columns, schema, None, Some(Compression::ZSTD))
}
