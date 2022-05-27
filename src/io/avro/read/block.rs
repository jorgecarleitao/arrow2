//! APIs to read from Avro format to arrow.
use std::io::Read;

use fallible_streaming_iterator::FallibleStreamingIterator;

use crate::error::{Error, Result};

use super::super::CompressedBlock;
use super::util;

fn read_size<R: Read>(reader: &mut R) -> Result<(usize, usize)> {
    let rows = match util::zigzag_i64(reader) {
        Ok(a) => a,
        Err(Error::Io(io_err)) => {
            if let std::io::ErrorKind::UnexpectedEof = io_err.kind() {
                // end
                return Ok((0, 0));
            } else {
                return Err(Error::Io(io_err));
            }
        }
        Err(other) => return Err(other),
    };
    let bytes = util::zigzag_i64(reader)?;
    Ok((rows as usize, bytes as usize))
}

/// Reads a [`CompressedBlock`] from the `reader`.
/// # Error
/// This function errors iff either the block cannot be read or the sync marker does not match
fn read_block<R: Read>(
    reader: &mut R,
    block: &mut CompressedBlock,
    file_marker: [u8; 16],
) -> Result<()> {
    let (rows, bytes) = read_size(reader)?;
    block.number_of_rows = rows;
    if rows == 0 {
        return Ok(());
    };

    block.data.clear();
    block.data.resize(bytes, 0);
    reader.read_exact(&mut block.data)?;

    let mut marker = [0u8; 16];
    reader.read_exact(&mut marker)?;

    if marker != file_marker {
        return Err(Error::ExternalFormat(
            "Avro: the sync marker in the block does not correspond to the file marker".to_string(),
        ));
    }
    Ok(())
}

/// [`FallibleStreamingIterator`] of compressed avro blocks
pub struct BlockStreamIterator<R: Read> {
    buf: CompressedBlock,
    reader: R,
    file_marker: [u8; 16],
}

impl<R: Read> BlockStreamIterator<R> {
    /// Creates a new [`BlockStreamIterator`].
    pub fn new(reader: R, file_marker: [u8; 16]) -> Self {
        Self {
            reader,
            file_marker,
            buf: CompressedBlock::new(0, vec![]),
        }
    }

    /// The buffer of [`BlockStreamIterator`].
    pub fn buffer(&mut self) -> &mut CompressedBlock {
        &mut self.buf
    }

    /// Deconstructs itself
    pub fn into_inner(self) -> (R, Vec<u8>) {
        (self.reader, self.buf.data)
    }
}

impl<R: Read> FallibleStreamingIterator for BlockStreamIterator<R> {
    type Error = Error;
    type Item = CompressedBlock;

    fn advance(&mut self) -> Result<()> {
        read_block(&mut self.reader, &mut self.buf, self.file_marker)?;
        Ok(())
    }

    fn get(&self) -> Option<&Self::Item> {
        if self.buf.number_of_rows > 0 {
            Some(&self.buf)
        } else {
            None
        }
    }
}
