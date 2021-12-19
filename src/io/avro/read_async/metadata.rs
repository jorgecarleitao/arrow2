//! Async Avro
use std::collections::HashMap;

use avro_schema::{Record, Schema as AvroSchema};
use futures::AsyncRead;
use futures::AsyncReadExt;

use crate::datatypes::Schema;
use crate::error::{ArrowError, Result};

use super::super::read::convert_schema;
use super::super::read::deserialize_header;
use super::super::read::Compression;
use super::super::{read_header, read_metadata};
use super::utils::zigzag_i64;

/// Reads Avro's metadata from `reader` into a [`AvroSchema`], [`Compression`] and magic marker.
#[allow(clippy::type_complexity)]
async fn read_metadata_async<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
) -> Result<(AvroSchema, Option<Compression>, [u8; 16])> {
    read_metadata!(reader.await)
}

/// Reads the avro metadata from `reader` into a [`AvroSchema`], [`Compression`] and magic marker.
#[allow(clippy::type_complexity)]
pub async fn read_metadata<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
) -> Result<(Vec<AvroSchema>, Schema, Option<Compression>, [u8; 16])> {
    let (avro_schema, codec, marker) = read_metadata_async(reader).await?;
    let schema = convert_schema(&avro_schema)?;

    let avro_schema = if let AvroSchema::Record(Record { fields, .. }) = avro_schema {
        fields.into_iter().map(|x| x.schema).collect()
    } else {
        panic!()
    };

    Ok((avro_schema, schema, codec, marker))
}

/// Reads the file marker asynchronously
async fn read_file_marker<R: AsyncRead + Unpin + Send>(reader: &mut R) -> Result<[u8; 16]> {
    let mut marker = [0u8; 16];
    reader.read_exact(&mut marker).await?;
    Ok(marker)
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
