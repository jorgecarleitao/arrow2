//! APIs to read from Avro format to arrow.
use std::io::Read;

use fallible_streaming_iterator::FallibleStreamingIterator;

use crate::error::{ArrowError, Result};

use super::super::{Block, CompressedBlock};
use super::BlockStreamIterator;
use super::Compression;

const CRC_TABLE: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);

/// Decompresses an Avro block.
/// Returns whether the buffers where swapped.
pub fn decompress_block(
    block: &mut CompressedBlock,
    decompressed: &mut Block,
    compression: Option<Compression>,
) -> Result<bool> {
    decompressed.number_of_rows = block.number_of_rows;
    let block = &mut block.data;
    let decompressed = &mut decompressed.data;

    match compression {
        None => {
            std::mem::swap(block, decompressed);
            Ok(true)
        }
        #[cfg(feature = "io_avro_compression")]
        Some(Compression::Deflate) => {
            decompressed.clear();
            let mut decoder = libflate::deflate::Decoder::new(&block[..]);
            decoder.read_to_end(decompressed)?;
            Ok(false)
        }
        #[cfg(feature = "io_avro_compression")]
        Some(Compression::Snappy) => {
            let crc = &block[block.len() - 4..];
            let block = &block[..block.len() - 4];

            let len = snap::raw::decompress_len(block)
                .map_err(|e| ArrowError::ExternalFormat(e.to_string()))?;
            decompressed.clear();
            decompressed.resize(len, 0);
            snap::raw::Decoder::new()
                .decompress(block, decompressed)
                .map_err(|e| ArrowError::ExternalFormat(e.to_string()))?;

            let expected_crc = u32::from_be_bytes([crc[0], crc[1], crc[2], crc[3]]);

            let actual_crc = CRC_TABLE.checksum(decompressed);
            if expected_crc != actual_crc {
                return Err(ArrowError::ExternalFormat(
                    "The crc of snap-compressed block does not match".to_string(),
                ));
            }
            Ok(false)
        }
        #[cfg(not(feature = "io_avro_compression"))]
        Some(Compression::Deflate) => Err(ArrowError::InvalidArgumentError(
            "The avro file is deflate-encoded but feature 'io_avro_compression' is not active."
                .to_string(),
        )),
        #[cfg(not(feature = "io_avro_compression"))]
        Some(Compression::Snappy) => Err(ArrowError::InvalidArgumentError(
            "The avro file is snappy-encoded but feature 'io_avro_compression' is not active."
                .to_string(),
        )),
    }
}

/// [`FallibleStreamingIterator`] of decompressed Avro blocks
pub struct Decompressor<R: Read> {
    blocks: BlockStreamIterator<R>,
    codec: Option<Compression>,
    buf: Block,
    was_swapped: bool,
}

impl<R: Read> Decompressor<R> {
    /// Creates a new [`Decompressor`].
    pub fn new(blocks: BlockStreamIterator<R>, codec: Option<Compression>) -> Self {
        Self {
            blocks,
            codec,
            buf: Block::new(0, vec![]),
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
    type Item = Block;

    fn advance(&mut self) -> Result<()> {
        if self.was_swapped {
            std::mem::swap(&mut self.blocks.buffer().data, &mut self.buf.data);
        }
        self.blocks.advance()?;
        self.was_swapped = decompress_block(self.blocks.buffer(), &mut self.buf, self.codec)?;
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
