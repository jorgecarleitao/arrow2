//! Async Avro
use std::collections::HashMap;

use avro_rs::Schema;
use futures::AsyncRead;
use futures::AsyncReadExt;

use crate::error::{ArrowError, Result};

use super::read::deserialize_header;
use super::read::Compression;
use super::{avro_decode, read_header, read_metadata};

/// Reads Avro's metadata from `reader` into a [`Schema`], [`Compression`] and magic marker.
#[allow(clippy::type_complexity)]
pub async fn read_metadata_async<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
) -> Result<(Schema, Option<Compression>, [u8; 16])> {
    read_metadata!(reader.await)
}

/// Reads the file marker asynchronously
async fn read_file_marker<R: AsyncRead + Unpin + Send>(reader: &mut R) -> Result<[u8; 16]> {
    let mut marker = [0u8; 16];
    reader.read_exact(&mut marker).await?;
    Ok(marker)
}

async fn zigzag_i64<R: AsyncRead + Unpin + Send>(reader: &mut R) -> Result<i64> {
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

async fn _read_binary<R: AsyncRead + Unpin + Send>(reader: &mut R) -> Result<Vec<u8>> {
    let len: usize = zigzag_i64(reader).await? as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn read_header<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
) -> Result<HashMap<String, Vec<u8>>> {
    read_header!(reader.await)
}
