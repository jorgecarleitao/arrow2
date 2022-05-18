use std::collections::HashMap;

use avro_schema::Schema;
use serde_json;

use crate::error::{Error, Result};

use super::Compression;

/// Serializes an [`Schema`] and optional [`Compression`] into an avro header.
pub(crate) fn serialize_header(
    schema: &Schema,
    compression: Option<Compression>,
) -> Result<HashMap<String, Vec<u8>>> {
    let schema =
        serde_json::to_string(schema).map_err(|e| Error::ExternalFormat(e.to_string()))?;

    let mut header = HashMap::<String, Vec<u8>>::default();

    header.insert("avro.schema".to_string(), schema.into_bytes());
    if let Some(compression) = compression {
        let value = match compression {
            Compression::Snappy => b"snappy".to_vec(),
            Compression::Deflate => b"deflate".to_vec(),
        };
        header.insert("avro.codec".to_string(), value);
    };

    Ok(header)
}
