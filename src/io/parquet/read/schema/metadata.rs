use std::collections::HashMap;

use parquet2::metadata::KeyValue;

use crate::datatypes::Schema;
use crate::error::{ArrowError, Result};
use crate::io::ipc;

const ARROW_SCHEMA_META_KEY: &str = "ARROW:schema";

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
            match ipc::root_as_message(slice) {
                Ok(message) => message
                    .header_as_schema()
                    .map(ipc::fb_to_schema)
                    .ok_or_else(|| ArrowError::Ipc("the message is not Arrow Schema".to_string())),
                Err(err) => {
                    // The flatbuffers implementation returns an error on verification error.
                    Err(ArrowError::Ipc(format!(
                        "Unable to get root as message stored in {}: {:?}",
                        ARROW_SCHEMA_META_KEY, err
                    )))
                }
            }
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

#[cfg(test)]
mod tests {
    use std::fs::File;

    use parquet2::read::read_metadata;

    use crate::datatypes::Schema;
    use crate::error::Result;

    use super::read_schema_from_metadata;

    fn read_schema(path: &str) -> Result<Option<Schema>> {
        let mut file = File::open(path).unwrap();

        let metadata = read_metadata(&mut file)?;
        let keys = metadata.key_value_metadata();
        read_schema_from_metadata(keys)
    }

    #[test]
    fn test_basic() -> Result<()> {
        if std::env::var("ARROW2_IGNORE_PARQUET").is_ok() {
            return Ok(());
        }
        let schema = read_schema("fixtures/pyarrow3/v1/basic_nullable_10.parquet")?;
        let names = schema
            .unwrap()
            .fields()
            .iter()
            .map(|x| x.name().clone())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec!["int64", "float64", "string", "bool", "date", "uint32"]
        );
        Ok(())
    }
}
