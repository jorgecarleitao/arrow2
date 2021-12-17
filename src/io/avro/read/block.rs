//! APIs to read from Avro format to arrow.
use std::io::Read;

use fallible_streaming_iterator::FallibleStreamingIterator;

use crate::error::{ArrowError, Result};

use super::util;

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

/// Reads a block from the `reader` into `buf`.
/// # Panic
/// Panics iff the block marker does not equal to the file's marker
fn read_block<R: Read>(reader: &mut R, buf: &mut Vec<u8>, file_marker: [u8; 16]) -> Result<usize> {
    let (rows, bytes) = read_size(reader)?;
    if rows == 0 {
        return Ok(0);
    };

    buf.clear();
    buf.resize(bytes, 0);
    reader.read_exact(buf)?;

    let mut marker = [0u8; 16];
    reader.read_exact(&mut marker)?;

    assert!(!(marker != file_marker));
    Ok(rows)
}

/// [`FallibleStreamingIterator`] of compressed avro blocks
pub struct BlockStreamIterator<R: Read> {
    buf: (Vec<u8>, usize),
    reader: R,
    file_marker: [u8; 16],
}

impl<R: Read> BlockStreamIterator<R> {
    /// Creates a new [`BlockStreamIterator`].
    pub fn new(reader: R, file_marker: [u8; 16]) -> Self {
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

    /// Deconstructs itself
    pub fn into_inner(self) -> (R, Vec<u8>) {
        (self.reader, self.buf.0)
    }
}

impl<R: Read> FallibleStreamingIterator for BlockStreamIterator<R> {
    type Error = ArrowError;
    type Item = (Vec<u8>, usize);

    fn advance(&mut self) -> Result<()> {
        let (buf, rows) = &mut self.buf;
        *rows = read_block(&mut self.reader, buf, self.file_marker)?;
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
