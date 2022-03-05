use odbc_api::buffers::{BinColumnIt, TextColumnIt};
use odbc_api::Bit;

use crate::array::{Array, BinaryArray, BooleanArray, PrimitiveArray, Utf8Array};
use crate::bitmap::{Bitmap, MutableBitmap};
use crate::buffer::Buffer;
use crate::datatypes::DataType;
use crate::types::NativeType;

use super::super::api::buffers::AnyColumnView;

/// Deserializes a [`AnyColumnView`] into an array of [`DataType`].
/// This is CPU-bounded
pub fn deserialize(column: AnyColumnView, data_type: DataType) -> Box<dyn Array> {
    match column {
        AnyColumnView::Text(iter) => Box::new(utf8(data_type, iter)) as _,
        AnyColumnView::WText(_) => todo!(),
        AnyColumnView::Binary(iter) => Box::new(binary(data_type, iter)) as _,
        AnyColumnView::Date(_) => todo!(),
        AnyColumnView::Time(_) => todo!(),
        AnyColumnView::Timestamp(_) => todo!(),
        AnyColumnView::F64(values) => Box::new(primitive(data_type, values)) as _,
        AnyColumnView::F32(values) => Box::new(primitive(data_type, values)) as _,
        AnyColumnView::I8(values) => Box::new(primitive(data_type, values)) as _,
        AnyColumnView::I16(values) => Box::new(primitive(data_type, values)) as _,
        AnyColumnView::I32(values) => Box::new(primitive(data_type, values)) as _,
        AnyColumnView::I64(values) => Box::new(primitive(data_type, values)) as _,
        AnyColumnView::U8(values) => Box::new(primitive(data_type, values)) as _,
        AnyColumnView::Bit(values) => Box::new(bool(data_type, values)) as _,
        AnyColumnView::NullableDate(_) => todo!(),
        AnyColumnView::NullableTime(_) => todo!(),
        AnyColumnView::NullableTimestamp(_) => todo!(),
        AnyColumnView::NullableF64(slice) => Box::new(primitive_optional(
            data_type,
            slice.raw_values().0,
            slice.raw_values().1,
        )) as _,
        AnyColumnView::NullableF32(slice) => Box::new(primitive_optional(
            data_type,
            slice.raw_values().0,
            slice.raw_values().1,
        )) as _,
        AnyColumnView::NullableI8(slice) => Box::new(primitive_optional(
            data_type,
            slice.raw_values().0,
            slice.raw_values().1,
        )) as _,
        AnyColumnView::NullableI16(slice) => Box::new(primitive_optional(
            data_type,
            slice.raw_values().0,
            slice.raw_values().1,
        )) as _,
        AnyColumnView::NullableI32(slice) => Box::new(primitive_optional(
            data_type,
            slice.raw_values().0,
            slice.raw_values().1,
        )) as _,
        AnyColumnView::NullableI64(slice) => Box::new(primitive_optional(
            data_type,
            slice.raw_values().0,
            slice.raw_values().1,
        )) as _,
        AnyColumnView::NullableU8(slice) => Box::new(primitive_optional(
            data_type,
            slice.raw_values().0,
            slice.raw_values().1,
        )) as _,
        AnyColumnView::NullableBit(slice) => Box::new(bool_optional(
            data_type,
            slice.raw_values().0,
            slice.raw_values().1,
        )) as _,
    }
}

fn bitmap(values: &[isize]) -> Option<Bitmap> {
    MutableBitmap::from_trusted_len_iter(values.iter().map(|x| *x != -1)).into()
}

fn primitive<T: NativeType>(data_type: DataType, values: &[T]) -> PrimitiveArray<T> {
    PrimitiveArray::from_data(data_type, values.to_vec().into(), None)
}

fn primitive_optional<T: NativeType>(
    data_type: DataType,
    values: &[T],
    indicators: &[isize],
) -> PrimitiveArray<T> {
    let validity = bitmap(indicators);
    PrimitiveArray::from_data(data_type, values.to_vec().into(), validity)
}

fn bool(data_type: DataType, values: &[Bit]) -> BooleanArray {
    let values = values.iter().map(|x| x.as_bool());
    let values = Bitmap::from_trusted_len_iter(values);
    BooleanArray::from_data(data_type, values, None)
}

fn bool_optional(data_type: DataType, values: &[Bit], indicators: &[isize]) -> BooleanArray {
    let validity = bitmap(indicators);
    let values = values.iter().map(|x| x.as_bool());
    let values = Bitmap::from_trusted_len_iter(values);
    BooleanArray::from_data(data_type, values, validity)
}

fn binary_generic<'a>(
    iter: impl Iterator<Item = Option<&'a [u8]>>,
) -> (Buffer<i32>, Buffer<u8>, Option<Bitmap>) {
    let length = iter.size_hint().0;
    let mut validity = MutableBitmap::with_capacity(length);
    let mut values = Vec::<u8>::with_capacity(0);

    let mut offsets = Vec::with_capacity(length + 1);
    offsets.push(0i32);

    for item in iter {
        if let Some(item) = item {
            values.extend_from_slice(item);
            validity.push(true);
        } else {
            validity.push(false);
        }
        offsets.push(values.len() as i32)
    }

    (offsets.into(), values.into(), validity.into())
}

fn binary(data_type: DataType, iter: BinColumnIt) -> BinaryArray<i32> {
    let (offsets, values, validity) = binary_generic(iter);

    // this O(N) check is not necessary
    BinaryArray::from_data(data_type, offsets, values, validity)
}

fn utf8(data_type: DataType, iter: TextColumnIt<u8>) -> Utf8Array<i32> {
    let (offsets, values, validity) = binary_generic(iter);

    // this O(N) check is necessary for the utf8 validity
    Utf8Array::from_data(data_type, offsets, values, validity)
}
