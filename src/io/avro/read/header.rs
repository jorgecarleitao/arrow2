use ahash::AHashMap;
use avro_schema::Schema;
use serde_json;

use crate::error::{Error, Result};

use super::Compression;

/// Deserializes the Avro header into an Avro [`Schema`] and optional [`Compression`].
pub(crate) fn deserialize_header(
    header: AHashMap<String, Vec<u8>>,
) -> Result<(Schema, Option<Compression>)> {
    let schema = header
        .get("avro.schema")
        .ok_or_else(|| Error::ExternalFormat("Avro schema must be present".to_string()))
        .and_then(|bytes| {
            serde_json::from_slice(bytes.as_ref()).map_err(|e| Error::ExternalFormat(e.to_string()))
        })?;

    let compression = header.get("avro.codec").and_then(|bytes| {
        let bytes: &[u8] = bytes.as_ref();
        match bytes {
            b"snappy" => Some(Compression::Snappy),
            b"deflate" => Some(Compression::Deflate),
            _ => None,
        }
    });
    Ok((schema, compression))
}
