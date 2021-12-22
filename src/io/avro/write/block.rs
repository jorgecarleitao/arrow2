use std::io::Write;

use crate::error::Result;

use super::super::CompressedBlock;
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
