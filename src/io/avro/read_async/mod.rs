//! Async Avro
use std::collections::HashMap;

use avro_rs::Schema;
use futures::AsyncRead;
use futures::AsyncReadExt;

use crate::error::{ArrowError, Result};

use super::read::deserialize_header;
use super::read::Compression;

/// Reads Avro's metadata from `reader` into a [`Schema`], [`Compression`] and magic marker.
#[allow(clippy::type_complexity)]
pub async fn read_metadata_async<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
) -> Result<(Schema, Option<Compression>, [u8; 16])> {
    let mut magic_number = [0u8; 4];
    reader.read_exact(&mut magic_number).await?;

    // see https://avro.apache.org/docs/current/spec.html#Object+Container+Files
    if magic_number != [b'O', b'b', b'j', 1u8] {
        return Err(ArrowError::ExternalFormat(
            "Avro header does not contain a valid magic number".to_string(),
        ));
    }

    let header = read_header(reader).await?;

    // this is blocking but we can't really live without it
    let (schema, compression) = deserialize_header(header)?;

    let marker = read_file_marker(reader).await?;

    Ok((schema, compression, marker))
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
    let mut i = 0u64;
    let mut buf = [0u8; 1];

    let mut j = 0;
    loop {
        if j > 9 {
            // if j * 7 > 64
            return Err(ArrowError::ExternalFormat(
                "zigzag decoding failed - corrupt avro file".to_string(),
            ));
        }
        reader.read_exact(&mut buf[..]).await?;
        i |= (u64::from(buf[0] & 0x7F)) << (j * 7);
        if (buf[0] >> 7) == 0 {
            break;
        } else {
            j += 1;
        }
    }

    Ok(i)
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
    let mut items = HashMap::new();

    loop {
        let len = zigzag_i64(reader).await? as usize;
        if len == 0 {
            break Ok(items);
        }

        items.reserve(len);
        for _ in 0..len {
            let key = _read_binary(reader).await?;
            let key = String::from_utf8(key)
                .map_err(|_| ArrowError::ExternalFormat("Invalid Avro header".to_string()))?;
            let value = _read_binary(reader).await?;
            items.insert(key, value);
        }
    }
}
