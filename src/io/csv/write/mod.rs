mod iterator;
mod serialize;

use iterator::StreamingIterator;

use std::io::Write;

// re-export necessary public APIs from csv
pub use csv::{ByteRecord, Writer, WriterBuilder};

pub use serialize::*;

use crate::record_batch::RecordBatch;
use crate::{datatypes::Schema, error::Result};

/// Creates serializers that iterate over each column of `batch` and serialize each item according
/// to `options`.
fn new_serializers<'a>(
    batch: &'a RecordBatch,
    options: &'a SerializeOptions,
) -> Result<Vec<Box<dyn StreamingIterator<Item = [u8]> + 'a>>> {
    batch
        .columns()
        .iter()
        .map(|column| new_serializer(column.as_ref(), options))
        .collect()
}

/// Serializes a [`RecordBatch`] as vector of `ByteRecord`.
/// The vector is guaranteed to have `batch.num_rows()` entries.
/// Each `ByteRecord` is guaranteed to have `batch.num_columns()` fields.
pub fn serialize(batch: &RecordBatch, options: &SerializeOptions) -> Result<Vec<ByteRecord>> {
    let mut serializers = new_serializers(batch, options)?;

    let mut records = vec![ByteRecord::with_capacity(0, batch.num_columns()); batch.num_rows()];
    records.iter_mut().for_each(|record| {
        serializers
            .iter_mut()
            // `unwrap` is infalible because `array.len()` equals `num_rows` on a `RecordBatch`
            .for_each(|iter| record.push_field(iter.next().unwrap()));
    });
    Ok(records)
}

/// Writes the data in a `RecordBatch` to `writer` according to the serialization options `options`.
pub fn write_batch<W: Write>(
    writer: &mut Writer<W>,
    batch: &RecordBatch,
    options: &SerializeOptions,
) -> Result<()> {
    let mut serializers = new_serializers(batch, options)?;

    let mut record = ByteRecord::with_capacity(0, batch.num_columns());

    // this is where the (expensive) transposition happens: the outer loop is on rows, the inner on columns
    (0..batch.num_rows()).try_for_each(|_| {
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
