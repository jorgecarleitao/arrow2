use std::sync::Arc;

use serde_json::Value;

use crate::array::Array;
use crate::datatypes::DataType;
use crate::error::Error;

use super::super::super::json::read::_deserialize;

/// Deserializes rows into an [`Array`] of [`DataType`].
/// # Implementation
/// This function is CPU-bounded.
/// This function is guaranteed to return an array of length equal to `rows.len()`.
/// # Errors
/// This function errors iff any of the rows is not a valid JSON (i.e. the format is not valid NDJSON).
pub fn deserialize(rows: &[String], data_type: DataType) -> Result<Arc<dyn Array>, Error> {
    if rows.is_empty() {
        return Err(Error::ExternalFormat(
            "Cannot deserialize 0 NDJSON rows because empty string is not a valid JSON value"
                .to_string(),
        ));
    }

    deserialize_iter(rows.iter(), data_type)
}

/// Deserializes an iterator of rows into an [`Array`] of [`DataType`].
/// # Implementation
/// This function is CPU-bounded.
/// This function is guaranteed to return an array of length equal to the leng
/// # Errors
/// This function errors iff any of the rows is not a valid JSON (i.e. the format is not valid NDJSON).
pub fn deserialize_iter<A: AsRef<str>>(
    rows: impl Iterator<Item = A>,
    data_type: DataType,
) -> Result<Arc<dyn Array>, Error> {
    // deserialize strings to `Value`s
    let rows = rows
        .map(|row| serde_json::from_str(row.as_ref()).map_err(Error::from))
        .collect::<Result<Vec<Value>, Error>>()?;

    // deserialize &[Value] to Array
    Ok(_deserialize(&rows, data_type))
}
