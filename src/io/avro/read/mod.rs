//! APIs to read from Avro format to arrow.
use std::io::Read;

use avro_schema::{Record, Schema as AvroSchema};
use fallible_streaming_iterator::FallibleStreamingIterator;

mod block;
mod decompress;
pub use block::BlockStreamIterator;
pub use decompress::{decompress_block, Decompressor};
mod deserialize;
pub use deserialize::deserialize;
mod header;
mod nested;
mod schema;
mod util;

pub(super) use header::deserialize_header;
pub(super) use schema::convert_schema;

use crate::array::Array;
use crate::columns::Columns;
use crate::datatypes::{Field, Schema};
use crate::error::Result;

use super::Compression;

/// Reads the avro metadata from `reader` into a [`Schema`], [`Compression`] and magic marker.
#[allow(clippy::type_complexity)]
pub fn read_metadata<R: std::io::Read>(
    reader: &mut R,
) -> Result<(Vec<AvroSchema>, Schema, Option<Compression>, [u8; 16])> {
    let (avro_schema, codec, marker) = util::read_schema(reader)?;
    let schema = convert_schema(&avro_schema)?;

    let avro_schema = if let AvroSchema::Record(Record { fields, .. }) = avro_schema {
        fields.into_iter().map(|x| x.schema).collect()
    } else {
        panic!()
    };

    Ok((avro_schema, schema, codec, marker))
}

/// Single threaded, blocking reader of Avro; [`Iterator`] of [`Columns`].
pub struct Reader<R: Read> {
    iter: Decompressor<R>,
    avro_schemas: Vec<AvroSchema>,
    fields: Vec<Field>,
}

impl<R: Read> Reader<R> {
    /// Creates a new [`Reader`].
    pub fn new(iter: Decompressor<R>, avro_schemas: Vec<AvroSchema>, fields: Vec<Field>) -> Self {
        Self {
            iter,
            avro_schemas,
            fields,
        }
    }

    /// Deconstructs itself into its internal reader
    pub fn into_inner(self) -> R {
        self.iter.into_inner()
    }
}

impl<R: Read> Iterator for Reader<R> {
    type Item = Result<Columns<Box<dyn Array>>>;

    fn next(&mut self) -> Option<Self::Item> {
        let fields = &self.fields[..];
        let avro_schemas = &self.avro_schemas;

        self.iter
            .next()
            .transpose()
            .map(|maybe_block| deserialize(maybe_block?, fields, avro_schemas))
    }
}
