use std::collections::HashMap;

use avro_rs::{Error, Schema};
use serde_json;

use crate::error::Result;

use super::Compression;

/// Deserializes the Avro header into an Avro [`Schema`] and optional [`Compression`].
pub(crate) fn deserialize_header(
    header: HashMap<String, Vec<u8>>,
) -> Result<(Schema, Option<Compression>)> {
    let json = header
        .get("avro.schema")
        .and_then(|bytes| serde_json::from_slice(bytes.as_ref()).ok())
        .ok_or(Error::GetAvroSchemaFromMap)?;
    let schema = Schema::parse(&json)?;

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
