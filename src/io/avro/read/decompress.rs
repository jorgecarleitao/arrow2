//! APIs to read from Avro format to arrow.
use std::io::Read;

use fallible_streaming_iterator::FallibleStreamingIterator;

use crate::error::{ArrowError, Result};

use super::BlockStreamIterator;
use super::Compression;

/// Decompresses an avro block.
/// Returns whether the buffers where swapped.
fn decompress_block(
    block: &mut Vec<u8>,
    decompress: &mut Vec<u8>,
    compression: Option<Compression>,
) -> Result<bool> {
    match compression {
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

/// [`FallibleStreamingIterator`] of decompressed Avro blocks
pub struct Decompressor<R: Read> {
    blocks: BlockStreamIterator<R>,
    codec: Option<Compression>,
    buf: (Vec<u8>, usize),
    was_swapped: bool,
}

impl<R: Read> Decompressor<R> {
    /// Creates a new [`Decompressor`].
    pub fn new(blocks: BlockStreamIterator<R>, codec: Option<Compression>) -> Self {
        Self {
            blocks,
            codec,
            buf: (vec![], 0),
            was_swapped: false,
        }
    }

    /// Deconstructs itself into its internal reader
    pub fn into_inner(self) -> R {
        self.blocks.into_inner().0
    }
}

impl<'a, R: Read> FallibleStreamingIterator for Decompressor<R> {
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
