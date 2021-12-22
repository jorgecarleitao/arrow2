//! APIs to read from Avro format to arrow.
use async_stream::try_stream;
use futures::AsyncRead;
use futures::AsyncReadExt;
use futures::Stream;

use crate::error::{ArrowError, Result};

use super::CompressedBlock;

use super::utils::zigzag_i64;

async fn read_size<R: AsyncRead + Unpin + Send>(reader: &mut R) -> Result<(usize, usize)> {
    let rows = match zigzag_i64(reader).await {
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
    let bytes = zigzag_i64(reader).await?;
    Ok((rows as usize, bytes as usize))
}

/// Reads a [`CompressedBlock`] from the `reader`.
/// # Error
/// This function errors iff either the block cannot be read or the sync marker does not match
async fn read_block<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
    block: &mut CompressedBlock,
    file_marker: [u8; 16],
) -> Result<()> {
    let (rows, bytes) = read_size(reader).await?;
    block.number_of_rows = rows;
    if rows == 0 {
        return Ok(());
    };

    block.data.clear();
    block.data.resize(bytes, 0);
    reader.read_exact(&mut block.data).await?;

    let mut marker = [0u8; 16];
    reader.read_exact(&mut marker).await?;

    if marker != file_marker {
        return Err(ArrowError::ExternalFormat(
            "Avro: the sync marker in the block does not correspond to the file marker".to_string(),
        ));
    }
    Ok(())
}

/// Returns a fallible [`Stream`] of Avro blocks bound to `reader`
pub async fn block_stream<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
    file_marker: [u8; 16],
) -> impl Stream<Item = Result<CompressedBlock>> + '_ {
    try_stream! {
        loop {
            let mut block = CompressedBlock::new(0, vec![]);
            read_block(reader, &mut block, file_marker).await?;
            if block.number_of_rows == 0 {
                break
            }
            yield block
        }
    }
}
