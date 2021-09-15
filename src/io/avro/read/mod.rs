use std::io::Read;
use std::sync::Arc;

use avro_rs::Codec;
use avro_rs::Reader as AvroReader;
use streaming_iterator::StreamingIterator;

mod deserialize;
mod schema;
mod util;

use crate::datatypes::Schema;
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;

pub fn read_metadata<R: std::io::Read>(reader: &mut R) -> Result<(Schema, Codec, [u8; 16])> {
    let (schema, codec, marker) = util::read_schema(reader)?;
    Ok((schema::convert_schema(&schema)?, codec, marker))
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

    if marker != file_marker {
        panic!();
    }
    Ok(rows)
}

fn decompress_block(buf: &mut Vec<u8>, decompress: &mut Vec<u8>, codec: Codec) -> Result<bool> {
    match codec {
        Codec::Null => {
            std::mem::swap(buf, decompress);
            Ok(false)
        }
        Codec::Deflate => {
            todo!()
        }
    }
}

/// [`StreamingIterator`] of blocks of avro data
pub struct BlockStreamIterator<'a, R: Read> {
    buf: (Vec<u8>, usize),
    reader: &'a mut R,
    file_marker: [u8; 16],
}

impl<'a, R: Read> BlockStreamIterator<'a, R> {
    pub fn new(reader: &'a mut R, file_marker: [u8; 16]) -> Self {
        Self {
            reader,
            file_marker,
            buf: (vec![], 0),
        }
    }

    pub fn buffer(&mut self) -> &mut Vec<u8> {
        &mut self.buf.0
    }
}

impl<'a, R: Read> StreamingIterator for BlockStreamIterator<'a, R> {
    type Item = (Vec<u8>, usize);

    fn advance(&mut self) {
        let (buf, rows) = &mut self.buf;
        // todo: surface this error
        *rows = read_block(self.reader, buf, self.file_marker).unwrap();
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
    codec: Codec,
    buf: (Vec<u8>, usize),
    was_swapped: bool,
}

impl<'a, R: Read> Decompressor<'a, R> {
    pub fn new(blocks: BlockStreamIterator<'a, R>, codec: Codec) -> Self {
        Self {
            blocks,
            codec,
            buf: (vec![], 0),
            was_swapped: false,
        }
    }
}

impl<'a, R: Read> StreamingIterator for Decompressor<'a, R> {
    type Item = (Vec<u8>, usize);

    fn advance(&mut self) {
        if self.was_swapped {
            std::mem::swap(self.blocks.buffer(), &mut self.buf.0);
        }
        self.blocks.advance();
        self.was_swapped =
            decompress_block(self.blocks.buffer(), &mut self.buf.0, self.codec).unwrap();
        self.buf.1 = self.blocks.get().map(|(_, rows)| *rows).unwrap_or_default();
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
}

impl<'a, R: Read> Reader<'a, R> {
    pub fn new(iter: Decompressor<'a, R>, schema: Arc<Schema>) -> Self {
        Self { iter, schema }
    }
}

impl<'a, R: Read> Iterator for Reader<'a, R> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((data, rows)) = self.iter.next() {
            Some(deserialize::deserialize(data, *rows, self.schema.clone()))
        } else {
            None
        }
    }
}
