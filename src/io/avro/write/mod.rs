//! APIs to write to Avro format.
use std::io::Write;

use avro_schema::{Field as AvroField, Record, Schema as AvroSchema};

use crate::error::Result;

pub use super::Compression;

mod header;
use header::serialize_header;
mod schema;
pub use schema::to_avro_schema;
mod serialize;
pub use serialize::{can_serialize, new_serializer, BoxSerializer};
mod block;
pub use block::*;
mod util;

const SYNC_NUMBER: [u8; 16] = [1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4];

/// Writes Avro's metadata to `writer`.
pub fn write_metadata<W: std::io::Write>(
    writer: &mut W,
    fields: Vec<AvroField>,
    compression: Option<Compression>,
) -> Result<()> {
    // * Four bytes, ASCII 'O', 'b', 'j', followed by 1.
    let avro_magic = [b'O', b'b', b'j', 1u8];
    writer.write_all(&avro_magic)?;

    // * file metadata, including the schema.
    let schema = AvroSchema::Record(Record::new("", fields));
    let header = serialize_header(&schema, compression)?;

    util::zigzag_encode(header.len() as i64, writer)?;
    for (name, item) in header {
        util::write_binary(name.as_bytes(), writer)?;
        util::write_binary(&item, writer)?;
    }
    writer.write_all(&[0])?;

    // The 16-byte, randomly-generated sync marker for this file.
    writer.write_all(&SYNC_NUMBER)?;

    Ok(())
}

/// consumes a set of [`BoxSerializer`] into an [`Block`].
/// # Panics
/// Panics iff the number of items in any of the serializers is not equal to the number of rows
/// declared in the `block`.
pub fn serialize<'a>(serializers: &mut [BoxSerializer<'a>], block: &mut Block) -> Result<()> {
    let Block {
        data,
        number_of_rows,
    } = block;

    data.clear(); // restart it

    // _the_ transpose (columns -> rows)
    for _ in 0..*number_of_rows {
        for serializer in &mut *serializers {
            let item_data = serializer.next().unwrap();
            data.write_all(item_data)?;
        }
    }
    Ok(())
}
