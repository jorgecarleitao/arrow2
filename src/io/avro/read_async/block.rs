//! APIs to read from Avro format to arrow.
use async_stream::try_stream;
use futures::AsyncRead;
use futures::AsyncReadExt;
use futures::Stream;

use crate::error::{ArrowError, Result};

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

/// Reads a block from the file into `buf`.
/// # Panic
/// Panics iff the block marker does not equal to the file's marker
async fn read_block<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    file_marker: [u8; 16],
) -> Result<usize> {
    let (rows, bytes) = read_size(reader).await?;
    if rows == 0 {
        return Ok(0);
    };

    buf.clear();
    buf.resize(bytes, 0);
    reader.read_exact(buf).await?;

    let mut marker = [0u8; 16];
    reader.read_exact(&mut marker).await?;

    assert!(!(marker != file_marker));
    Ok(rows)
}

/// Returns a fallible [`Stream`] of Avro blocks bound to `reader`
pub async fn block_stream<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
    file_marker: [u8; 16],
) -> impl Stream<Item = Result<(Vec<u8>, usize)>> + '_ {
    try_stream! {
        loop {
            let mut buffer = vec![];
            let rows = read_block(reader, &mut buffer, file_marker).await?;
            if rows == 0 {
                break
            }
            yield (buffer, rows)
        }
    }
}
