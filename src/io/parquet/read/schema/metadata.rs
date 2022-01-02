use std::collections::HashMap;

pub use parquet2::metadata::KeyValue;

use crate::datatypes::Schema;
use crate::error::{ArrowError, Result};
use crate::io::ipc::read::deserialize_schema;

use super::super::super::ARROW_SCHEMA_META_KEY;

/// Reads an arrow schema from Parquet's file metadata. Returns `None` if no schema was found.
/// # Errors
/// Errors iff the schema cannot be correctly parsed.
pub fn read_schema_from_metadata(
    key_value_metadata: &Option<Vec<KeyValue>>,
) -> Result<Option<Schema>> {
    let mut metadata = parse_key_value_metadata(key_value_metadata).unwrap_or_default();
    metadata
        .remove(ARROW_SCHEMA_META_KEY)
        .map(|encoded| get_arrow_schema_from_metadata(&encoded))
        .transpose()
}

/// Try to convert Arrow schema metadata into a schema
fn get_arrow_schema_from_metadata(encoded_meta: &str) -> Result<Schema> {
    let decoded = base64::decode(encoded_meta);
    match decoded {
        Ok(bytes) => {
            let slice = if bytes[0..4] == [255u8; 4] {
                &bytes[8..]
            } else {
                bytes.as_slice()
            };
            deserialize_schema(slice).map(|x| x.0)
        }
        Err(err) => {
            // The C++ implementation returns an error if the schema can't be parsed.
            Err(ArrowError::InvalidArgumentError(format!(
                "Unable to decode the encoded schema stored in {}, {:?}",
                ARROW_SCHEMA_META_KEY, err
            )))
        }
    }
}

fn parse_key_value_metadata(
    key_value_metadata: &Option<Vec<KeyValue>>,
) -> Option<HashMap<String, String>> {
    match key_value_metadata {
        Some(key_values) => {
            let map: HashMap<String, String> = key_values
                .iter()
                .filter_map(|kv| {
                    kv.value
                        .as_ref()
                        .map(|value| (kv.key.clone(), value.clone()))
                })
                .collect();

            if map.is_empty() {
                None
            } else {
                Some(map)
            }
        }
        None => None,
    }
}
