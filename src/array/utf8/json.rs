use crate::{
    datatypes::DataType,
    array::ArrayRef,
    error::{ArrowError, Result},
    io::json::read::{_deserialize, coerce_data_type},
};
use super::{Utf8Array, Offset};


/// Implements json deserialization from a Utf8Array
#[cfg(feature = "io_json")]
impl<O: Offset> Utf8Array<O> {

    /// Infers the DataType from a number of JSON rows in a Utf8Array
    pub fn json_infer(&self, number_of_rows: Option<usize>) -> Result<DataType> {
        unimplemented!();
    }

    /// Deserializes JSON values based on an optional DataType
    pub fn json_deserialize(&self, data_type: DataType) -> Result<ArrayRef> {
        unimplemented!()
    }
}
