use std::sync::Arc;

use crate::datatypes::DataType;
use crate::{array::*, ffi};

use crate::error::Result;

/// Trait describing how a struct presents itself to the
/// [C data interface](https://arrow.apache.org/docs/format/CDataInterface.html) (FFI).
pub unsafe trait ToFfi {
    /// The pointers to the buffers.
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>>;

    /// The offset
    fn offset(&self) -> usize;

    /// The children
    fn children(&self) -> Vec<Arc<dyn Array>> {
        vec![]
    }
}

/// Trait describing how a struct imports into itself from the
/// [C data interface](https://arrow.apache.org/docs/format/CDataInterface.html) (FFI).
pub unsafe trait FromFfi<T: ffi::ArrowArrayRef>: Sized {
    fn try_from_ffi(array: T) -> Result<Self>;
}

macro_rules! ffi_dyn {
    ($array:expr, $ty:ty) => {{
        let array = $array.as_any().downcast_ref::<$ty>().unwrap();
        (array.buffers(), array.children())
    }};
}

pub fn buffers_children(
    array: &dyn Array,
) -> (Vec<Option<std::ptr::NonNull<u8>>>, Vec<Arc<dyn Array>>) {
    match array.data_type() {
        DataType::Null => ffi_dyn!(array, NullArray),
        DataType::Boolean => ffi_dyn!(array, BooleanArray),
        DataType::Int8 => ffi_dyn!(array, PrimitiveArray<i8>),
        DataType::Int16 => ffi_dyn!(array, PrimitiveArray<i16>),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            ffi_dyn!(array, PrimitiveArray<i32>)
        }
        DataType::Interval(IntervalUnit::DayTime) => ffi_dyn!(array, PrimitiveArray<days_ms>),
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => ffi_dyn!(array, PrimitiveArray<i64>),
        DataType::Decimal(_, _) => ffi_dyn!(array, PrimitiveArray<i128>),
        DataType::UInt8 => ffi_dyn!(array, PrimitiveArray<u8>),
        DataType::UInt16 => ffi_dyn!(array, PrimitiveArray<u16>),
        DataType::UInt32 => ffi_dyn!(array, PrimitiveArray<u32>),
        DataType::UInt64 => ffi_dyn!(array, PrimitiveArray<u64>),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => ffi_dyn!(array, PrimitiveArray<f32>),
        DataType::Float64 => ffi_dyn!(array, PrimitiveArray<f64>),
        DataType::Binary => ffi_dyn!(array, BinaryArray<i32>),
        DataType::LargeBinary => ffi_dyn!(array, BinaryArray<i64>),
        DataType::FixedSizeBinary(_) => ffi_dyn!(array, FixedSizeBinaryArray),
        DataType::Utf8 => ffi_dyn!(array, Utf8Array::<i32>),
        DataType::LargeUtf8 => ffi_dyn!(array, Utf8Array::<i64>),
        DataType::List(_) => ffi_dyn!(array, ListArray::<i32>),
        DataType::LargeList(_) => ffi_dyn!(array, ListArray::<i64>),
        DataType::FixedSizeList(_, _) => ffi_dyn!(array, FixedSizeListArray),
        DataType::Struct(_) => ffi_dyn!(array, StructArray),
        DataType::Union(_) => unimplemented!(),
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::Int8 => ffi_dyn!(array, DictionaryArray::<i8>),
            DataType::Int16 => ffi_dyn!(array, DictionaryArray::<i16>),
            DataType::Int32 => ffi_dyn!(array, DictionaryArray::<i32>),
            DataType::Int64 => ffi_dyn!(array, DictionaryArray::<i64>),
            DataType::UInt8 => ffi_dyn!(array, DictionaryArray::<u8>),
            DataType::UInt16 => ffi_dyn!(array, DictionaryArray::<u16>),
            DataType::UInt32 => ffi_dyn!(array, DictionaryArray::<u32>),
            DataType::UInt64 => ffi_dyn!(array, DictionaryArray::<u64>),
            _ => unreachable!(),
        },
    }
}
