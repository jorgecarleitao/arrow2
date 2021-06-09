use crate::array::*;
use crate::bitmap::Bitmap;
use crate::datatypes::{DataType, IntervalUnit};
use crate::types::days_ms;

fn validity_size(validity: &Option<Bitmap>) -> usize {
    validity.as_ref().map(|b| b.bytes().len()).unwrap_or(0)
}

macro_rules! dyn_primitive {
    ($array:expr, $ty:ty) => {{
        let array = $array
            .as_any()
            .downcast_ref::<PrimitiveArray<$ty>>()
            .unwrap();

        array.values().len() * std::mem::size_of::<$ty>() + validity_size(array.validity())
    }};
}

macro_rules! dyn_binary {
    ($array:expr, $ty:ty, $o:ty) => {{
        let array = $array.as_any().downcast_ref::<$ty>().unwrap();

        array.values().len()
            + array.offsets().len() * std::mem::size_of::<$o>()
            + validity_size(array.validity())
    }};
}

macro_rules! dyn_dict {
    ($array:expr, $ty:ty) => {{
        let array = $array
            .as_any()
            .downcast_ref::<DictionaryArray<$ty>>()
            .unwrap();
        estimated_bytes_size(array.keys()) + estimated_bytes_size(array.values().as_ref())
    }};
}

/// Returns the total (heap) allocated size of the array in bytes.
/// # Implementation
/// This estimation is the sum of the size of its buffers, validity, including nested arrays.
/// Multiple arrays may share buffers and bitmaps. Therefore, the size of 2 arrays is not the
/// sum of the sizes computed from this function. In particular, [`StructArray`]'s size is an upper bound.
///
/// When an array is sliced, its allocated size remains constant because the buffer unchanged.
/// However, this function will yield a smaller number. This is because this function returns
/// the visible size of the buffer, not its total capacity.
///
/// FFI buffers are included in this estimation.
pub fn estimated_bytes_size(array: &dyn Array) -> usize {
    use DataType::*;
    match array.data_type() {
        Null => 0,
        Boolean => {
            let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            array.values().bytes().len() + validity_size(array.validity())
        }
        Int8 => dyn_primitive!(array, i8),
        Int16 => dyn_primitive!(array, i16),
        Int32 | Date32 | Time32(_) | Interval(IntervalUnit::YearMonth) => {
            dyn_primitive!(array, i32)
        }
        Int64 | Date64 | Timestamp(_, _) | Time64(_) | Duration(_) => dyn_primitive!(array, i64),
        UInt8 => dyn_primitive!(array, u16),
        UInt16 => dyn_primitive!(array, u16),
        UInt32 => dyn_primitive!(array, u32),
        UInt64 => dyn_primitive!(array, u64),
        Float16 => unreachable!(),
        Float32 => dyn_primitive!(array, f32),
        Float64 => dyn_primitive!(array, f64),
        Decimal(_, _) => dyn_primitive!(array, i128),
        Interval(IntervalUnit::DayTime) => dyn_primitive!(array, days_ms),
        Binary => dyn_binary!(array, BinaryArray<i32>, i32),
        FixedSizeBinary(_) => {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            array.values().len() + validity_size(array.validity())
        }
        LargeBinary => dyn_binary!(array, BinaryArray<i64>, i64),
        Utf8 => dyn_binary!(array, Utf8Array<i32>, i32),
        LargeUtf8 => dyn_binary!(array, Utf8Array<i64>, i64),
        List(_) => {
            let array = array.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            estimated_bytes_size(array.values().as_ref())
                + array.offsets().len() * std::mem::size_of::<i32>()
                + validity_size(array.validity())
        }
        FixedSizeList(_, _) => {
            let array = array.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            estimated_bytes_size(array.values().as_ref()) + validity_size(array.validity())
        }
        LargeList(_) => {
            let array = array.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            estimated_bytes_size(array.values().as_ref())
                + array.offsets().len() * std::mem::size_of::<i64>()
                + validity_size(array.validity())
        }
        Struct(_) => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            array
                .values()
                .iter()
                .map(|x| x.as_ref())
                .map(estimated_bytes_size)
                .sum::<usize>()
                + validity_size(array.validity())
        }
        Union(_) => unreachable!(),
        Dictionary(keys, _) => match keys.as_ref() {
            Int8 => dyn_dict!(array, i8),
            Int16 => dyn_dict!(array, i16),
            Int32 => dyn_dict!(array, i32),
            Int64 => dyn_dict!(array, i64),
            UInt8 => dyn_dict!(array, u8),
            UInt16 => dyn_dict!(array, u16),
            UInt32 => dyn_dict!(array, u32),
            UInt64 => dyn_dict!(array, u64),
            _ => unreachable!(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primitive() {
        let a = Int32Array::from_slice(&[1, 2, 3, 4, 5]);
        assert_eq!(5 * std::mem::size_of::<i32>(), estimated_bytes_size(&a));
    }

    #[test]
    fn boolean() {
        let a = BooleanArray::from_slice(&[true]);
        assert_eq!(1, estimated_bytes_size(&a));
    }

    #[test]
    fn utf8() {
        let a = Utf8Array::<i32>::from_slice(&["aaa"]);
        assert_eq!(3 + 2 * std::mem::size_of::<i32>(), estimated_bytes_size(&a));
    }
}
