use futures::{AsyncWrite, AsyncWriteExt};

use crate::error::Result;

use super::super::write::{util::zigzag_encode, SYNC_NUMBER};
use super::super::CompressedBlock;

/// Writes a [`CompressedBlock`] to `writer`
pub async fn write_block<W>(writer: &mut W, compressed_block: &CompressedBlock) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    // write size and rows
    let mut scratch = Vec::with_capacity(10);
    zigzag_encode(compressed_block.number_of_rows as i64, &mut scratch)?;
    writer.write_all(&scratch).await?;
    scratch.clear();
    zigzag_encode(compressed_block.data.len() as i64, &mut scratch)?;
    writer.write_all(&scratch).await?;

    writer.write_all(&compressed_block.data).await?;

    writer.write_all(&SYNC_NUMBER).await?;

    Ok(())
}
