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

macro_rules! ffi_buffers_dyn {
    ($array:expr, $ty:ty) => {{
        let array = $array.as_any().downcast_ref::<$ty>().unwrap();
        array.buffers()
    }};
}

macro_rules! ffi_children_dyn {
    ($array:expr, $ty:ty) => {{
        let array = $array.as_any().downcast_ref::<$ty>().unwrap();
        array.children()
    }};
}

macro_rules! ffi_offset_dyn {
    ($array:expr, $ty:ty) => {{
        let array = $array.as_any().downcast_ref::<$ty>().unwrap();
        array.offset()
    }};
}

type BuffersChildren = (
    Vec<Option<std::ptr::NonNull<u8>>>,
    Vec<Arc<dyn Array>>,
    Option<Arc<dyn Array>>,
);

pub fn buffers(array: &dyn Array) -> Vec<Option<std::ptr::NonNull<u8>>> {
    match array.data_type() {
        DataType::Null => ffi_buffers_dyn!(array, NullArray),
        DataType::Boolean => ffi_buffers_dyn!(array, BooleanArray),
        DataType::Int8 => ffi_buffers_dyn!(array, PrimitiveArray<i8>),
        DataType::Int16 => ffi_buffers_dyn!(array, PrimitiveArray<i16>),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            ffi_buffers_dyn!(array, PrimitiveArray<i32>)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            ffi_buffers_dyn!(array, PrimitiveArray<days_ms>)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => ffi_buffers_dyn!(array, PrimitiveArray<i64>),
        DataType::Decimal(_, _) => ffi_buffers_dyn!(array, PrimitiveArray<i128>),
        DataType::UInt8 => ffi_buffers_dyn!(array, PrimitiveArray<u8>),
        DataType::UInt16 => ffi_buffers_dyn!(array, PrimitiveArray<u16>),
        DataType::UInt32 => ffi_buffers_dyn!(array, PrimitiveArray<u32>),
        DataType::UInt64 => ffi_buffers_dyn!(array, PrimitiveArray<u64>),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => ffi_buffers_dyn!(array, PrimitiveArray<f32>),
        DataType::Float64 => ffi_buffers_dyn!(array, PrimitiveArray<f64>),
        DataType::Binary => ffi_buffers_dyn!(array, BinaryArray<i32>),
        DataType::LargeBinary => ffi_buffers_dyn!(array, BinaryArray<i64>),
        DataType::FixedSizeBinary(_) => ffi_buffers_dyn!(array, FixedSizeBinaryArray),
        DataType::Utf8 => ffi_buffers_dyn!(array, Utf8Array::<i32>),
        DataType::LargeUtf8 => ffi_buffers_dyn!(array, Utf8Array::<i64>),
        DataType::List(_) => ffi_buffers_dyn!(array, ListArray::<i32>),
        DataType::LargeList(_) => ffi_buffers_dyn!(array, ListArray::<i64>),
        DataType::FixedSizeList(_, _) => ffi_buffers_dyn!(array, FixedSizeListArray),
        DataType::Struct(_) => ffi_buffers_dyn!(array, StructArray),
        DataType::Union(_, _, _) => ffi_buffers_dyn!(array, UnionArray),
        DataType::Dictionary(key_type, _) => {
            with_match_dictionary_key_type!(key_type.as_ref(), |$T| {
                let array = array.as_any().downcast_ref::<DictionaryArray<$T>>().unwrap();
                array.buffers()
            })
        }
        DataType::Extension(_, _, _) => ffi_buffers_dyn!(array, ExtensionArray),
    }
}

pub fn children(array: &dyn Array) -> Vec<Arc<dyn Array>> {
    match array.data_type() {
        DataType::Null => ffi_children_dyn!(array, NullArray),
        DataType::Boolean => ffi_children_dyn!(array, BooleanArray),
        DataType::Int8 => ffi_children_dyn!(array, PrimitiveArray<i8>),
        DataType::Int16 => ffi_children_dyn!(array, PrimitiveArray<i16>),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            ffi_children_dyn!(array, PrimitiveArray<i32>)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            ffi_children_dyn!(array, PrimitiveArray<days_ms>)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => ffi_children_dyn!(array, PrimitiveArray<i64>),
        DataType::Decimal(_, _) => ffi_children_dyn!(array, PrimitiveArray<i128>),
        DataType::UInt8 => ffi_children_dyn!(array, PrimitiveArray<u8>),
        DataType::UInt16 => ffi_children_dyn!(array, PrimitiveArray<u16>),
        DataType::UInt32 => ffi_children_dyn!(array, PrimitiveArray<u32>),
        DataType::UInt64 => ffi_children_dyn!(array, PrimitiveArray<u64>),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => ffi_children_dyn!(array, PrimitiveArray<f32>),
        DataType::Float64 => ffi_children_dyn!(array, PrimitiveArray<f64>),
        DataType::Binary => ffi_children_dyn!(array, BinaryArray<i32>),
        DataType::LargeBinary => ffi_children_dyn!(array, BinaryArray<i64>),
        DataType::FixedSizeBinary(_) => ffi_children_dyn!(array, FixedSizeBinaryArray),
        DataType::Utf8 => ffi_children_dyn!(array, Utf8Array::<i32>),
        DataType::LargeUtf8 => ffi_children_dyn!(array, Utf8Array::<i64>),
        DataType::List(_) => ffi_children_dyn!(array, ListArray::<i32>),
        DataType::LargeList(_) => ffi_children_dyn!(array, ListArray::<i64>),
        DataType::FixedSizeList(_, _) => ffi_children_dyn!(array, FixedSizeListArray),
        DataType::Struct(_) => ffi_children_dyn!(array, StructArray),
        DataType::Union(_, _, _) => ffi_children_dyn!(array, UnionArray),
        DataType::Dictionary(key_type, _) => {
            with_match_dictionary_key_type!(key_type.as_ref(), |$T| {
                let array = array.as_any().downcast_ref::<DictionaryArray<$T>>().unwrap();
                    array.children()
            })
        }
        DataType::Extension(_, _, _) => ffi_children_dyn!(array, ExtensionArray),
    }
}

pub fn offset(array: &dyn Array) -> usize {
    match array.data_type() {
        DataType::Null => ffi_offset_dyn!(array, NullArray),
        DataType::Boolean => ffi_offset_dyn!(array, BooleanArray),
        DataType::Int8 => ffi_offset_dyn!(array, PrimitiveArray<i8>),
        DataType::Int16 => ffi_offset_dyn!(array, PrimitiveArray<i16>),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            ffi_offset_dyn!(array, PrimitiveArray<i32>)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            ffi_offset_dyn!(array, PrimitiveArray<days_ms>)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => ffi_offset_dyn!(array, PrimitiveArray<i64>),
        DataType::Decimal(_, _) => ffi_offset_dyn!(array, PrimitiveArray<i128>),
        DataType::UInt8 => ffi_offset_dyn!(array, PrimitiveArray<u8>),
        DataType::UInt16 => ffi_offset_dyn!(array, PrimitiveArray<u16>),
        DataType::UInt32 => ffi_offset_dyn!(array, PrimitiveArray<u32>),
        DataType::UInt64 => ffi_offset_dyn!(array, PrimitiveArray<u64>),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => ffi_offset_dyn!(array, PrimitiveArray<f32>),
        DataType::Float64 => ffi_offset_dyn!(array, PrimitiveArray<f64>),
        DataType::Binary => ffi_offset_dyn!(array, BinaryArray<i32>),
        DataType::LargeBinary => ffi_offset_dyn!(array, BinaryArray<i64>),
        DataType::FixedSizeBinary(_) => ffi_offset_dyn!(array, FixedSizeBinaryArray),
        DataType::Utf8 => ffi_offset_dyn!(array, Utf8Array::<i32>),
        DataType::LargeUtf8 => ffi_offset_dyn!(array, Utf8Array::<i64>),
        DataType::List(_) => ffi_offset_dyn!(array, ListArray::<i32>),
        DataType::LargeList(_) => ffi_offset_dyn!(array, ListArray::<i64>),
        DataType::FixedSizeList(_, _) => ffi_offset_dyn!(array, FixedSizeListArray),
        DataType::Struct(_) => ffi_offset_dyn!(array, StructArray),
        DataType::Union(_, _, _) => ffi_offset_dyn!(array, UnionArray),
        DataType::Dictionary(key_type, _) => {
            with_match_dictionary_key_type!(key_type.as_ref(), |$T| {
                let array = array.as_any().downcast_ref::<DictionaryArray<$T>>().unwrap();
                    array.offset()
            })
        }
        DataType::Extension(_, _, _) => ffi_offset_dyn!(array, ExtensionArray),
    }
}

pub fn dictionary(array: &dyn Array) -> Option<Arc<dyn Array>> {
    match array.data_type() {
        DataType::Dictionary(key_type, _) => {
            with_match_dictionary_key_type!(key_type.as_ref(), |$T| {
                let array = array.as_any().downcast_ref::<DictionaryArray<$T>>().unwrap();
                Some(array.values().clone())
            })
        }
        DataType::Extension(_, _, _) => {
            let array = array.as_any().downcast_ref::<ExtensionArray>().unwrap();
            dictionary(array.inner())
        }
        _ => None,
    }
}

pub fn buffers_children_dictionary(array: &dyn Array) -> BuffersChildren {
    (buffers(array), children(array), dictionary(array))
}
