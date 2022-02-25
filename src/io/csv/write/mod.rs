//! APIs to write to CSV
mod serialize;

use super::super::iterator::StreamingIterator;

use std::io::Write;

// re-export necessary public APIs from csv
pub use csv::{ByteRecord, WriterBuilder};

pub use serialize::*;

use crate::array::Array;
use crate::chunk::Chunk;
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

/// Serializes [`Chunk`] to a vector of `ByteRecord`.
/// The vector is guaranteed to have `columns.len()` entries.
/// Each `ByteRecord` is guaranteed to have `columns.array().len()` fields.
pub fn serialize<A: AsRef<dyn Array>>(
    columns: &Chunk<A>,
    options: &SerializeOptions,
) -> Result<Vec<ByteRecord>> {
    let mut serializers = new_serializers(columns, options)?;

    let rows = columns.len();
    let mut records = vec![ByteRecord::with_capacity(0, columns.arrays().len()); rows];
    records.iter_mut().for_each(|record| {
        serializers
            .iter_mut()
            // `unwrap` is infalible because `array.len()` equals `len` in `Chunk::len`
            .for_each(|iter| record.push_field(iter.next().unwrap()));
    });
    Ok(records)
}

/// Writes [`Chunk`] to `writer` according to the serialization options `options`.
pub fn write_chunk<W: Write, A: AsRef<dyn Array>>(
    writer: &mut W,
    columns: &Chunk<A>,
    options: &SerializeOptions,
) -> Result<()> {
    let mut serializers = new_serializers(columns.arrays(), options)?;

    let rows = columns.len();
    let mut row = Vec::with_capacity(columns.arrays().len() * 10);

    // this is where the (expensive) transposition happens: the outer loop is on rows, the inner on columns
    (0..rows).try_for_each(|_| {
        serializers
            .iter_mut()
            // `unwrap` is infalible because `array.len()` equals `Chunk::len`
            .for_each(|iter| {
                let field = iter.next().unwrap();
                row.extend_from_slice(field);
                row.push(options.delimiter);
            });
        // replace last delimiter with new line
        let last_byte = row.len() - 1;
        row[last_byte] = b'\n';
        writer.write_all(&row)?;
        row.clear();
        Result::Ok(())
    })?;
    Ok(())
}

/// Writes a CSV header to `writer`
pub fn write_header<W: Write, T>(
    writer: &mut W,
    names: &[T],
    options: &SerializeOptions,
) -> Result<()>
where
    T: AsRef<str>,
{
    let names = names.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
    writer.write_all(
        names
            .join(std::str::from_utf8(&[options.delimiter]).unwrap())
            .as_bytes(),
    )?;
    writer.write_all(&[b'\n'])?;
    Ok(())
}
