//! Declares the [`is_in`] operator

use crate::{
    array::{Array, BinaryArray, BooleanArray, PrimitiveArray, Utf8Array},
    bitmap::Bitmap,
    datatypes::DataType,
    error::{Error, Result},
    offset::Offset,
    types::NativeType,
};

fn is_in_primitive<T: NativeType>(
    values: &PrimitiveArray<T>,
    list: &PrimitiveArray<T>,
) -> Result<BooleanArray> {
    let values = values.iter().map(|value| list.iter().any(|x| x == value));
    let values = Bitmap::from_trusted_len_iter(values);

    Ok(BooleanArray::new(DataType::Boolean, values, None))
}

fn is_in_utf8<O: Offset>(values: &Utf8Array<O>, list: &Utf8Array<O>) -> Result<BooleanArray> {
    let values = values.iter().map(|value| list.iter().any(|x| x == value));
    let values = Bitmap::from_trusted_len_iter(values);

    Ok(BooleanArray::new(DataType::Boolean, values, None))
}

fn is_in_binary<O: Offset>(values: &BinaryArray<O>, list: &BinaryArray<O>) -> Result<BooleanArray> {
    let values = values.iter().map(|value| list.iter().any(|x| x == value));
    let values = Bitmap::from_trusted_len_iter(values);

    Ok(BooleanArray::new(DataType::Boolean, values, None))
}

macro_rules! primitive {
    ($values:expr, $list:expr, $ty:ty) => {{
        let list = $list
            .as_any()
            .downcast_ref::<PrimitiveArray<$ty>>()
            .unwrap();
        let values = $values
            .as_any()
            .downcast_ref::<PrimitiveArray<$ty>>()
            .unwrap();
        is_in_primitive(values, list)
    }};
}

/// Returns whether each element in `values` is in `list`
pub fn is_in(values: &dyn Array, list: &dyn Array) -> Result<BooleanArray> {
    let list_data_type = list.data_type();
    let values_data_type = values.data_type();

    match (values_data_type, list_data_type) {
        (DataType::Utf8, DataType::Utf8) => {
            let list = list.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            let values = values.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            is_in_utf8(values, list)
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            let list = list.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            let values = values.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            is_in_utf8(values, list)
        }
        (DataType::Binary, DataType::Binary) => {
            let list = list.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();
            let values = values.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();
            is_in_binary(values, list)
        }
        (DataType::LargeBinary, DataType::LargeBinary) => {
            let list = list.as_any().downcast_ref::<BinaryArray<i64>>().unwrap();
            let values = values.as_any().downcast_ref::<BinaryArray<i64>>().unwrap();
            is_in_binary(values, list)
        }
        (DataType::Int8, DataType::Int8) => primitive!(values, list, i8),
        (DataType::Int16, DataType::Int16) => primitive!(values, list, i16),
        (DataType::Int32, DataType::Int32) => primitive!(values, list, i32),
        (DataType::Int64, DataType::Int64) => primitive!(values, list, i64),
        (DataType::UInt8, DataType::UInt8) => primitive!(values, list, u8),
        (DataType::UInt16, DataType::UInt16) => primitive!(values, list, u16),
        (DataType::UInt32, DataType::UInt32) => primitive!(values, list, u32),
        (DataType::UInt64, DataType::UInt64) => primitive!(values, list, u64),
        (DataType::Float32, DataType::Float32) => primitive!(values, list, f32),
        (DataType::Float64, DataType::Float64) => primitive!(values, list, f64),
        _ => Err(Error::NotYetImplemented(format!(
            "is_in is not supported between logical types \"{values_data_type:?}\" and \"{list_data_type:?}\""
        ))),
    }
}
