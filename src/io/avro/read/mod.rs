#![deny(missing_docs)]
//! APIs to read from Avro format to arrow.
use std::io::Read;
use std::sync::Arc;

use avro_rs::Schema as AvroSchema;
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

use crate::datatypes::Schema;
use crate::error::Result;
use crate::record_batch::RecordBatch;

/// Valid compressions
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum Compression {
    /// Deflate
    Deflate,
    /// Snappy
    Snappy,
}

/// Reads the avro metadata from `reader` into a [`Schema`], [`Compression`] and magic marker.
#[allow(clippy::type_complexity)]
pub fn read_metadata<R: std::io::Read>(
    reader: &mut R,
) -> Result<(Vec<AvroSchema>, Schema, Option<Compression>, [u8; 16])> {
    let (avro_schema, codec, marker) = util::read_schema(reader)?;
    let schema = schema::convert_schema(&avro_schema)?;

    let avro_schema = if let AvroSchema::Record { fields, .. } = avro_schema {
        fields.into_iter().map(|x| x.schema).collect()
    } else {
        panic!()
    };

    Ok((avro_schema, schema, codec, marker))
}

/// Single threaded, blocking reader of Avro; [`Iterator`] of [`RecordBatch`]es.
pub struct Reader<R: Read> {
    iter: Decompressor<R>,
    schema: Arc<Schema>,
    avro_schemas: Vec<AvroSchema>,
}

impl<R: Read> Reader<R> {
    /// Creates a new [`Reader`].
    pub fn new(iter: Decompressor<R>, avro_schemas: Vec<AvroSchema>, schema: Arc<Schema>) -> Self {
        Self {
            iter,
            avro_schemas,
            schema,
        }
    }

    /// Deconstructs itself into its internal reader
    pub fn into_inner(self) -> R {
        self.iter.into_inner()
    }
}

impl<R: Read> Iterator for Reader<R> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let schema = self.schema.clone();
        let avro_schemas = &self.avro_schemas;

        self.iter.next().transpose().map(|x| {
            let (data, rows) = x?;
            deserialize(data, *rows, schema, avro_schemas)
        })
    }
}
