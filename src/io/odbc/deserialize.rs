use odbc_api::Bit;

use crate::array::{Array, BooleanArray, PrimitiveArray};
use crate::bitmap::{Bitmap, MutableBitmap};
use crate::datatypes::DataType;
use crate::types::NativeType;

use super::api::buffers::AnyColumnView;

/// Deserializes a [`AnyColumnView`] into an array of [`DataType`].
/// This is CPU-bounded
pub fn deserialize(column: AnyColumnView, data_type: DataType) -> Box<dyn Array> {
    match column {
        AnyColumnView::Text(_) => todo!(),
        AnyColumnView::WText(_) => todo!(),
        AnyColumnView::Binary(_) => todo!(),
        AnyColumnView::Date(_) => todo!(),
        AnyColumnView::Time(_) => todo!(),
        AnyColumnView::Timestamp(_) => todo!(),
        AnyColumnView::F64(values) => Box::new(deserialize_p(data_type, values)) as _,
        AnyColumnView::F32(values) => Box::new(deserialize_p(data_type, values)) as _,
        AnyColumnView::I8(values) => Box::new(deserialize_p(data_type, values)) as _,
        AnyColumnView::I16(values) => Box::new(deserialize_p(data_type, values)) as _,
        AnyColumnView::I32(values) => Box::new(deserialize_p(data_type, values)) as _,
        AnyColumnView::I64(values) => Box::new(deserialize_p(data_type, values)) as _,
        AnyColumnView::U8(values) => Box::new(deserialize_p(data_type, values)) as _,
        AnyColumnView::Bit(values) => Box::new(deserialize_bool(data_type, values)) as _,
        AnyColumnView::NullableDate(_) => todo!(),
        AnyColumnView::NullableTime(_) => todo!(),
        AnyColumnView::NullableTimestamp(_) => todo!(),
        AnyColumnView::NullableF64(slice) => Box::new(deserialize_p_optional(
            data_type,
            slice.values(),
            slice.indicators(),
        )) as _,
        AnyColumnView::NullableF32(slice) => Box::new(deserialize_p_optional(
            data_type,
            slice.values(),
            slice.indicators(),
        )) as _,
        AnyColumnView::NullableI8(slice) => Box::new(deserialize_p_optional(
            data_type,
            slice.values(),
            slice.indicators(),
        )) as _,
        AnyColumnView::NullableI16(slice) => Box::new(deserialize_p_optional(
            data_type,
            slice.values(),
            slice.indicators(),
        )) as _,
        AnyColumnView::NullableI32(slice) => Box::new(deserialize_p_optional(
            data_type,
            slice.values(),
            slice.indicators(),
        )) as _,
        AnyColumnView::NullableI64(slice) => Box::new(deserialize_p_optional(
            data_type,
            slice.values(),
            slice.indicators(),
        )) as _,
        AnyColumnView::NullableU8(slice) => Box::new(deserialize_p_optional(
            data_type,
            slice.values(),
            slice.indicators(),
        )) as _,
        AnyColumnView::NullableBit(slice) => Box::new(deserialize_bool_optional(
            data_type,
            slice.values(),
            slice.indicators(),
        )) as _,
    }
}

fn deserialize_bitmap(values: &[isize]) -> Option<Bitmap> {
    MutableBitmap::from_trusted_len_iter(values.iter().map(|x| *x != -1)).into()
}

fn deserialize_p<T: NativeType>(data_type: DataType, values: &[T]) -> PrimitiveArray<T> {
    PrimitiveArray::from_data(data_type, values.to_vec().into(), None)
}

fn deserialize_p_optional<T: NativeType>(
    data_type: DataType,
    values: &[T],
    indicators: &[isize],
) -> PrimitiveArray<T> {
    let validity = deserialize_bitmap(indicators);
    PrimitiveArray::from_data(data_type, values.to_vec().into(), validity)
}

fn deserialize_bool(data_type: DataType, values: &[Bit]) -> BooleanArray {
    let values = values.iter().map(|x| x.as_bool());
    let values = Bitmap::from_trusted_len_iter(values);
    BooleanArray::from_data(data_type, values, None)
}

fn deserialize_bool_optional(
    data_type: DataType,
    values: &[Bit],
    indicators: &[isize],
) -> BooleanArray {
    let validity = deserialize_bitmap(indicators);
    let values = values.iter().map(|x| x.as_bool());
    let values = Bitmap::from_trusted_len_iter(values);
    BooleanArray::from_data(data_type, values, validity)
}
