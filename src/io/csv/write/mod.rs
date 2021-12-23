//! APIs to write to CSV
mod serialize;

use super::super::iterator::StreamingIterator;

use std::io::Write;

// re-export necessary public APIs from csv
pub use csv::{ByteRecord, Writer, WriterBuilder};

pub use serialize::*;

use crate::array::Array;
use crate::{datatypes::Schema, error::Result};

/// Creates serializers that iterate over each column of `batch` and serialize each item according
/// to `options`.
fn new_serializers<'a, A: AsRef<dyn Array>>(
    batch: &'a [A],
    options: &'a SerializeOptions,
) -> Result<Vec<Box<dyn StreamingIterator<Item = [u8]> + 'a>>> {
    batch
        .iter()
        .map(|column| new_serializer(column.as_ref(), options))
        .collect()
}

/// Serializes a [`Array`]s as vector of `ByteRecord`.
/// The vector is guaranteed to have `batch.num_rows()` entries.
/// Each `ByteRecord` is guaranteed to have `batch.num_columns()` fields.
pub fn serialize<A: AsRef<dyn Array>>(
    batch: &[A],
    options: &SerializeOptions,
) -> Result<Vec<ByteRecord>> {
    let mut serializers = new_serializers(batch, options)?;

    let rows = batch[0].as_ref().len();
    let mut records = vec![ByteRecord::with_capacity(0, batch.len()); rows];
    records.iter_mut().for_each(|record| {
        serializers
            .iter_mut()
            // `unwrap` is infalible because `array.len()` equals `num_rows` on a `RecordBatch`
            .for_each(|iter| record.push_field(iter.next().unwrap()));
    });
    Ok(records)
}

/// Writes the data in a `RecordBatch` to `writer` according to the serialization options `options`.
pub fn write_batch<W: Write, A: AsRef<dyn Array>>(
    writer: &mut Writer<W>,
    batch: &[A],
    options: &SerializeOptions,
) -> Result<()> {
    let mut serializers = new_serializers(batch, options)?;

    let rows = batch[0].as_ref().len();
    let mut record = ByteRecord::with_capacity(0, batch.len());

    // this is where the (expensive) transposition happens: the outer loop is on rows, the inner on columns
    (0..rows).try_for_each(|_| {
        serializers
            .iter_mut()
            // `unwrap` is infalible because `array.len()` equals `num_rows` on a `RecordBatch`
            .for_each(|iter| record.push_field(iter.next().unwrap()));
        writer.write_byte_record(&record)?;
        record.clear();
        Result::Ok(())
    })?;
    Ok(())
}

/// Writes a header to `writer` according to `schema`
pub fn write_header<W: Write>(writer: &mut Writer<W>, schema: &Schema) -> Result<()> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    writer.write_record(&fields)?;
    Ok(())
}
