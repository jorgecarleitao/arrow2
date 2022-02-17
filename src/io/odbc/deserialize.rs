use odbc_api::Bit;

use crate::array::{Array, BinaryArray, BooleanArray, PrimitiveArray, Utf8Array};
use crate::bitmap::{Bitmap, MutableBitmap};
use crate::buffer::Buffer;
use crate::datatypes::DataType;
use crate::types::NativeType;

use super::api::buffers::AnyColumnView;

/// Deserializes a [`AnyColumnView`] into an array of [`DataType`].
/// This is CPU-bounded
pub fn deserialize(column: AnyColumnView, data_type: DataType) -> Box<dyn Array> {
    match column {
        AnyColumnView::Text(slice) => Box::new(utf8(
            data_type,
            slice.values(),
            slice.lengths(),
            slice.max_len(),
        )) as _,
        AnyColumnView::WText(_) => todo!(),
        AnyColumnView::Binary(slice) => Box::new(binary(
            data_type,
            slice.values(),
            slice.lengths(),
            slice.max_len(),
        )) as _,
        AnyColumnView::Date(_) => todo!(),
        AnyColumnView::Time(_) => todo!(),
        AnyColumnView::Timestamp(_) => todo!(),
        AnyColumnView::F64(values) => Box::new(p(data_type, values)) as _,
        AnyColumnView::F32(values) => Box::new(p(data_type, values)) as _,
        AnyColumnView::I8(values) => Box::new(p(data_type, values)) as _,
        AnyColumnView::I16(values) => Box::new(p(data_type, values)) as _,
        AnyColumnView::I32(values) => Box::new(p(data_type, values)) as _,
        AnyColumnView::I64(values) => Box::new(p(data_type, values)) as _,
        AnyColumnView::U8(values) => Box::new(p(data_type, values)) as _,
        AnyColumnView::Bit(values) => Box::new(bool(data_type, values)) as _,
        AnyColumnView::NullableDate(_) => todo!(),
        AnyColumnView::NullableTime(_) => todo!(),
        AnyColumnView::NullableTimestamp(_) => todo!(),
        AnyColumnView::NullableF64(slice) => {
            Box::new(p_optional(data_type, slice.values(), slice.indicators())) as _
        }
        AnyColumnView::NullableF32(slice) => {
            Box::new(p_optional(data_type, slice.values(), slice.indicators())) as _
        }
        AnyColumnView::NullableI8(slice) => {
            Box::new(p_optional(data_type, slice.values(), slice.indicators())) as _
        }
        AnyColumnView::NullableI16(slice) => {
            Box::new(p_optional(data_type, slice.values(), slice.indicators())) as _
        }
        AnyColumnView::NullableI32(slice) => {
            Box::new(p_optional(data_type, slice.values(), slice.indicators())) as _
        }
        AnyColumnView::NullableI64(slice) => {
            Box::new(p_optional(data_type, slice.values(), slice.indicators())) as _
        }
        AnyColumnView::NullableU8(slice) => {
            Box::new(p_optional(data_type, slice.values(), slice.indicators())) as _
        }
        AnyColumnView::NullableBit(slice) => {
            Box::new(bool_optional(data_type, slice.values(), slice.indicators())) as _
        }
    }
}

fn bitmap(values: &[isize]) -> Option<Bitmap> {
    MutableBitmap::from_trusted_len_iter(values.iter().map(|x| *x != -1)).into()
}

fn p<T: NativeType>(data_type: DataType, values: &[T]) -> PrimitiveArray<T> {
    PrimitiveArray::from_data(data_type, values.to_vec().into(), None)
}

fn p_optional<T: NativeType>(
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

fn binary_generic(
    slice: &[u8],
    lengths: &[isize],
    max_length: usize,
    null_terminator: usize,
) -> (Buffer<i32>, Buffer<u8>, Option<Bitmap>) {
    let mut validity = MutableBitmap::with_capacity(lengths.len());

    println!("{:?}", lengths);
    println!("{:?}", slice);
    let mut offsets = Vec::with_capacity(lengths.len() + 1);
    offsets.push(0i32);
    let mut length = 0;
    offsets.extend(lengths.iter().map(|&indicator| {
        validity.push(indicator != -1);
        length += if indicator > 0 { indicator as i32 } else { 0 };
        length
    }));
    // the loop above ensures monotonicity
    // this proves boundness
    assert!((length as usize) < slice.len());

    let mut values = Vec::<u8>::with_capacity(length as usize);
    offsets.windows(2).enumerate().for_each(|(index, x)| {
        let len = (x[1] - x[0]) as usize;
        let offset = index * (max_length + null_terminator);
        // this bound check is not necessary
        values.extend_from_slice(&slice[offset..offset + len])
    });

    // this O(N) check is not necessary

    (offsets.into(), values.into(), validity.into())
}

fn binary(
    data_type: DataType,
    slice: &[u8],
    lengths: &[isize],
    max_length: usize,
) -> BinaryArray<i32> {
    let (offsets, values, validity) = binary_generic(slice, lengths, max_length, 0);

    // this O(N) check is not necessary
    BinaryArray::from_data(data_type, offsets, values, validity)
}

fn utf8(data_type: DataType, slice: &[u8], lengths: &[isize], max_length: usize) -> Utf8Array<i32> {
    let (offsets, values, validity) = binary_generic(slice, lengths, max_length, 1);

    // this O(N) check is necessary for the utf8 validity
    Utf8Array::from_data(data_type, offsets, values, validity)
}
