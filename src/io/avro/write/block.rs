use std::io::Write;

use crate::{error::Result, io::avro::Compression};

use super::super::{Block, CompressedBlock};
use super::{util::zigzag_encode, SYNC_NUMBER};

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
    compressed_block.number_of_rows = block.number_of_rows;
    match compression {
        Compression::Deflate => todo!(),
        Compression::Snappy => todo!(),
    }
}
