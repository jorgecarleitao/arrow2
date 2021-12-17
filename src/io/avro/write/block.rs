use std::io::Write;

use crate::{error::Result, io::avro::Compression};

use super::{util::zigzag_encode, SYNC_NUMBER};

/// A compressed Avro block.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CompressedBlock {
    /// The number of rows
    pub number_of_rows: usize,
    /// The compressed data
    pub data: Vec<u8>,
}

impl CompressedBlock {
    /// Creates a new CompressedBlock
    pub fn new(number_of_rows: usize, data: Vec<u8>) -> Self {
        Self {
            number_of_rows,
            data,
        }
    }
}

/// An uncompressed Avro block.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Block {
    /// The number of rows
    pub number_of_rows: usize,
    /// The uncompressed data
    pub data: Vec<u8>,
}

impl Block {
    /// Creates a new Block
    pub fn new(number_of_rows: usize, data: Vec<u8>) -> Self {
        Self {
            number_of_rows,
            data,
        }
    }
}

/// Writes a [`CompressedBlock`] to `writer`
pub fn write_block<W: Write>(writer: &mut W, compressed_block: &CompressedBlock) -> Result<()> {
    // write size and rows
    zigzag_encode(compressed_block.number_of_rows as i64, writer)?;
    zigzag_encode(compressed_block.data.len() as i64, writer)?;

    writer.write_all(&compressed_block.data)?;

    writer.write_all(&SYNC_NUMBER)?;

    Ok(())
}

/// Compresses an [`Block`] to a [`CompressedBlock`].
pub fn compress(
    block: &Block,
    compressed_block: &mut CompressedBlock,
    compression: Compression,
) -> Result<()> {
    match compression {
        Compression::Deflate => todo!(),
        Compression::Snappy => todo!(),
    }
    compressed_block.number_of_rows = block.number_of_rows;
    Ok(())
}
