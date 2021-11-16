#![deny(missing_docs)]
//! APIs to read from Avro format to arrow.
use std::io::Read;
use std::sync::Arc;

use avro_rs::Schema as AvroSchema;
use fallible_streaming_iterator::FallibleStreamingIterator;

mod deserialize;
mod nested;
mod schema;
mod util;

use crate::datatypes::Schema;
use crate::error::{ArrowError, Result};
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

fn read_size<R: Read>(reader: &mut R) -> Result<(usize, usize)> {
    let rows = match util::zigzag_i64(reader) {
        Ok(a) => a,
        Err(ArrowError::Io(io_err)) => {
            if let std::io::ErrorKind::UnexpectedEof = io_err.kind() {
                // end
                return Ok((0, 0));
            } else {
                return Err(ArrowError::Io(io_err));
            }
        }
        Err(other) => return Err(other),
    };
    let bytes = util::zigzag_i64(reader)?;
    Ok((rows as usize, bytes as usize))
}

/// Reads a block from the file into `buf`.
/// # Panic
/// Panics iff the block marker does not equal to the file's marker
fn read_block<R: Read>(reader: &mut R, buf: &mut Vec<u8>, file_marker: [u8; 16]) -> Result<usize> {
    let (rows, bytes) = read_size(reader)?;
    if rows == 0 {
        return Ok(0);
    };

    buf.resize(bytes, 0);
    reader.read_exact(buf)?;

    let mut marker = [0u8; 16];
    reader.read_exact(&mut marker)?;

    assert!(!(marker != file_marker));
    Ok(rows)
}

/// Decompresses an avro block.
/// Returns whether the buffers where swapped.
fn decompress_block(
    block: &mut Vec<u8>,
    decompress: &mut Vec<u8>,
    codec: Option<Compression>,
) -> Result<bool> {
    match codec {
        None => {
            std::mem::swap(block, decompress);
            Ok(true)
        }
        #[cfg(feature = "io_avro_compression")]
        Some(Compression::Deflate) => {
            decompress.clear();
            let mut decoder = libflate::deflate::Decoder::new(&block[..]);
            decoder.read_to_end(decompress)?;
            Ok(false)
        }
        #[cfg(feature = "io_avro_compression")]
        Some(Compression::Snappy) => {
            let len = snap::raw::decompress_len(&block[..block.len() - 4])
                .map_err(|_| ArrowError::Other("Failed to decompress snap".to_string()))?;
            decompress.clear();
            decompress.resize(len, 0);
            snap::raw::Decoder::new()
                .decompress(&block[..block.len() - 4], decompress)
                .map_err(|_| ArrowError::Other("Failed to decompress snap".to_string()))?;
            Ok(false)
        }
        #[cfg(not(feature = "io_avro_compression"))]
        Some(Compression::Deflate) => Err(ArrowError::Other(
            "The avro file is deflate-encoded but feature 'io_avro_compression' is not active."
                .to_string(),
        )),
        #[cfg(not(feature = "io_avro_compression"))]
        Some(Compression::Snappy) => Err(ArrowError::Other(
            "The avro file is snappy-encoded but feature 'io_avro_compression' is not active."
                .to_string(),
        )),
    }
}

/// [`StreamingIterator`] of blocks of avro data
pub struct BlockStreamIterator<'a, R: Read> {
    buf: (Vec<u8>, usize),
    reader: &'a mut R,
    file_marker: [u8; 16],
}

impl<'a, R: Read> BlockStreamIterator<'a, R> {
    /// Creates a new [`BlockStreamIterator`].
    pub fn new(reader: &'a mut R, file_marker: [u8; 16]) -> Self {
        Self {
            reader,
            file_marker,
            buf: (vec![], 0),
        }
    }

    /// The buffer of [`BlockStreamIterator`].
    pub fn buffer(&mut self) -> &mut Vec<u8> {
        &mut self.buf.0
    }
}

impl<'a, R: Read> FallibleStreamingIterator for BlockStreamIterator<'a, R> {
    type Error = ArrowError;
    type Item = (Vec<u8>, usize);

    fn advance(&mut self) -> Result<()> {
        let (buf, rows) = &mut self.buf;
        *rows = read_block(self.reader, buf, self.file_marker)?;
        Ok(())
    }

    fn get(&self) -> Option<&Self::Item> {
        if self.buf.1 > 0 {
            Some(&self.buf)
        } else {
            None
        }
    }
}

/// [`StreamingIterator`] of blocks of decompressed avro data
pub struct Decompressor<'a, R: Read> {
    blocks: BlockStreamIterator<'a, R>,
    codec: Option<Compression>,
    buf: (Vec<u8>, usize),
    was_swapped: bool,
}

impl<'a, R: Read> Decompressor<'a, R> {
    /// Creates a new [`Decompressor`].
    pub fn new(blocks: BlockStreamIterator<'a, R>, codec: Option<Compression>) -> Self {
        Self {
            blocks,
            codec,
            buf: (vec![], 0),
            was_swapped: false,
        }
    }
}

impl<'a, R: Read> FallibleStreamingIterator for Decompressor<'a, R> {
    type Error = ArrowError;
    type Item = (Vec<u8>, usize);

    fn advance(&mut self) -> Result<()> {
        if self.was_swapped {
            std::mem::swap(self.blocks.buffer(), &mut self.buf.0);
        }
        self.blocks.advance()?;
        self.was_swapped = decompress_block(self.blocks.buffer(), &mut self.buf.0, self.codec)?;
        self.buf.1 = self.blocks.get().map(|(_, rows)| *rows).unwrap_or_default();
        Ok(())
    }

    fn get(&self) -> Option<&Self::Item> {
        if self.buf.1 > 0 {
            Some(&self.buf)
        } else {
            None
        }
    }
}

/// Single threaded, blocking reader of Avro files; [`Iterator`] of [`RecordBatch`]es.
pub struct Reader<'a, R: Read> {
    iter: Decompressor<'a, R>,
    schema: Arc<Schema>,
    avro_schemas: Vec<AvroSchema>,
}

impl<'a, R: Read> Reader<'a, R> {
    /// Creates a new [`Reader`].
    pub fn new(
        iter: Decompressor<'a, R>,
        avro_schemas: Vec<AvroSchema>,
        schema: Arc<Schema>,
    ) -> Self {
        Self {
            iter,
            avro_schemas,
            schema,
        }
    }
}

impl<'a, R: Read> Iterator for Reader<'a, R> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let schema = self.schema.clone();
        let avro_schemas = &self.avro_schemas;

        self.iter.next().transpose().map(|x| {
            let (data, rows) = x?;
            deserialize::deserialize(data, *rows, schema, avro_schemas)
        })
    }
}
