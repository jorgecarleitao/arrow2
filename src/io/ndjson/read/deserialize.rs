use std::sync::Arc;

use serde_json::Value;

use crate::array::Array;
use crate::datatypes::DataType;
use crate::error::ArrowError;

use super::super::super::json::read::_deserialize;

/// Deserializes rows into an [`Array`] of [`DataType`].
/// # Implementation
/// This function is CPU-bounded.
/// This function is guaranteed to return an array of length equal to `rows.len()`.
/// # Errors
/// This function errors iff any of the rows is not a valid JSON (i.e. the format is not valid NDJSON).
pub fn deserialize(rows: &[String], data_type: DataType) -> Result<Arc<dyn Array>, ArrowError> {
    // deserialize strings to `Value`s
    let rows = rows
        .iter()
        .map(|row| serde_json::from_str(row.as_ref()).map_err(ArrowError::from))
        .collect::<Result<Vec<Value>, ArrowError>>()?;

    // deserialize &[Value] to Array
    Ok(_deserialize(&rows, data_type))
}
