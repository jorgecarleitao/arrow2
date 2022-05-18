use futures::AsyncRead;
use futures::AsyncReadExt;

use crate::error::{Error, Result};

use super::super::avro_decode;

pub async fn zigzag_i64<R: AsyncRead + Unpin + Send>(reader: &mut R) -> Result<i64> {
    let z = decode_variable(reader).await?;
    Ok(if z & 0x1 == 0 {
        (z >> 1) as i64
    } else {
        !(z >> 1) as i64
    })
}

async fn decode_variable<R: AsyncRead + Unpin + Send>(reader: &mut R) -> Result<u64> {
    avro_decode!(reader.await)
}
