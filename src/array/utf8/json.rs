use indexmap::set::IndexSet as HashSet;
use serde_json;
use serde_json::Value;

use super::{Offset, Utf8Array};
use crate::{
    array::ArrayRef,
    datatypes::DataType,
    error::{ArrowError, Result},
    io::json::read::{_deserialize, coerce_data_type, infer},
};

/// Implements json deserialization from a Utf8Array
impl<O: Offset> Utf8Array<O> {
    /// Infers the DataType from a number of JSON rows in a Utf8Array
    pub fn json_infer(&self, number_of_rows: Option<usize>) -> Result<DataType> {
        if self.len() == 0 {
            return Err(ArrowError::ExternalFormat(
                "Cannot infer JSON types on empty Utf8Array".to_string(),
            ));
        }

        // Use the full length if no limit is provided
        let number_of_rows = number_of_rows.unwrap_or(self.len());

        let data_types_iter = self.iter().take(number_of_rows);

        let mut data_types = HashSet::new();
        for row in data_types_iter {
            if let Some(row) = row {
                let v: Value = serde_json::from_str(&row)?;
                let data_type = infer(&v)?;
                if data_type != DataType::Null {
                    data_types.insert(data_type);
                }
            }
        }

        let v: Vec<&DataType> = data_types.iter().collect();
        Ok(coerce_data_type(&v))
    }

    /// Deserializes JSON values based on an optional DataType
    pub fn json_deserialize(&self, data_type: DataType) -> Result<ArrayRef> {
        let rows = self
            .iter()
            .map(|row| match row {
                Some(row) => serde_json::from_str(row.as_ref()).map_err(ArrowError::from),
                None => Ok(Value::Null),
            })
            .collect::<Result<Vec<Value>>>()?;

        Ok(_deserialize(&rows, data_type))
    }
}
