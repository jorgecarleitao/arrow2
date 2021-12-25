//! APIs to write to CSV
mod serialize;

use super::super::iterator::StreamingIterator;

use std::io::Write;

// re-export necessary public APIs from csv
pub use csv::{ByteRecord, Writer, WriterBuilder};

pub use serialize::*;

use crate::array::Array;
use crate::columns::Columns;
use crate::error::Result;

/// Creates serializers that iterate over each column that serializes each item according
/// to `options`.
fn new_serializers<'a, A: AsRef<dyn Array>>(
    columns: &'a [A],
    options: &'a SerializeOptions,
) -> Result<Vec<Box<dyn StreamingIterator<Item = [u8]> + 'a>>> {
    columns
        .iter()
        .map(|column| new_serializer(column.as_ref(), options))
        .collect()
}

/// Serializes [`Columns`] to a vector of `ByteRecord`.
/// The vector is guaranteed to have `columns.len()` entries.
/// Each `ByteRecord` is guaranteed to have `columns.array().len()` fields.
pub fn serialize<A: AsRef<dyn Array>>(
    columns: &Columns<A>,
    options: &SerializeOptions,
) -> Result<Vec<ByteRecord>> {
    let mut serializers = new_serializers(columns, options)?;

    let rows = columns.len();
    let mut records = vec![ByteRecord::with_capacity(0, columns.arrays().len()); rows];
    records.iter_mut().for_each(|record| {
        serializers
            .iter_mut()
            // `unwrap` is infalible because `array.len()` equals `len` in `Columns::len`
            .for_each(|iter| record.push_field(iter.next().unwrap()));
    });
    Ok(records)
}

/// Writes [`Columns`] to `writer` according to the serialization options `options`.
pub fn write_batch<W: Write, A: AsRef<dyn Array>>(
    writer: &mut Writer<W>,
    columns: &Columns<A>,
    options: &SerializeOptions,
) -> Result<()> {
    let mut serializers = new_serializers(columns.arrays(), options)?;

    let rows = columns.len();
    let mut record = ByteRecord::with_capacity(0, columns.arrays().len());

    // this is where the (expensive) transposition happens: the outer loop is on rows, the inner on columns
    (0..rows).try_for_each(|_| {
        serializers
            .iter_mut()
            // `unwrap` is infalible because `array.len()` equals `Columns::len`
            .for_each(|iter| record.push_field(iter.next().unwrap()));
        writer.write_byte_record(&record)?;
        record.clear();
        Result::Ok(())
    })?;
    Ok(())
}

/// Writes a header to `writer`
pub fn write_header<W: Write, T, I>(writer: &mut Writer<W>, names: &[T]) -> Result<()>
where
    T: AsRef<str>,
{
    writer.write_record(names.iter().map(|x| x.as_ref().as_bytes()))?;
    Ok(())
}
