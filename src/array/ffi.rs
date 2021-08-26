use std::sync::Arc;

use crate::datatypes::{DataType, PhysicalDataType};
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
        (array.buffers(), array.children(), None)
    }};
}

type BuffersChildren = (
    Vec<Option<std::ptr::NonNull<u8>>>,
    Vec<Arc<dyn Array>>,
    Option<Arc<dyn Array>>,
);

pub fn buffers_children_dictionary(array: &dyn Array) -> BuffersChildren {
    let physical_type = array.data_type().to_physical_type();
    match physical_type {
        PhysicalDataType::Null => ffi_dyn!(array, NullArray),
        PhysicalDataType::Boolean => ffi_dyn!(array, BooleanArray),
        PhysicalDataType::Int8 => ffi_dyn!(array, PrimitiveArray<i8>),
        PhysicalDataType::Int16 => ffi_dyn!(array, PrimitiveArray<i16>),
        PhysicalDataType::Int32 => {
            ffi_dyn!(array, PrimitiveArray<i32>)
        }
        PhysicalDataType::DaysMs => {
            ffi_dyn!(array, PrimitiveArray<days_ms>)
        }
        PhysicalDataType::Int64 => ffi_dyn!(array, PrimitiveArray<i64>),
        PhysicalDataType::Int128 => ffi_dyn!(array, PrimitiveArray<i128>),
        PhysicalDataType::UInt8 => ffi_dyn!(array, PrimitiveArray<u8>),
        PhysicalDataType::UInt16 => ffi_dyn!(array, PrimitiveArray<u16>),
        PhysicalDataType::UInt32 => ffi_dyn!(array, PrimitiveArray<u32>),
        PhysicalDataType::UInt64 => ffi_dyn!(array, PrimitiveArray<u64>),
        PhysicalDataType::Float16 => unreachable!(),
        PhysicalDataType::Float32 => ffi_dyn!(array, PrimitiveArray<f32>),
        PhysicalDataType::Float64 => ffi_dyn!(array, PrimitiveArray<f64>),
        PhysicalDataType::Binary => ffi_dyn!(array, BinaryArray<i32>),
        PhysicalDataType::LargeBinary => ffi_dyn!(array, BinaryArray<i64>),
        PhysicalDataType::FixedSizeBinary(_) => ffi_dyn!(array, FixedSizeBinaryArray),
        PhysicalDataType::Utf8 => ffi_dyn!(array, Utf8Array::<i32>),
        PhysicalDataType::LargeUtf8 => ffi_dyn!(array, Utf8Array::<i64>),
        PhysicalDataType::List(_) => ffi_dyn!(array, ListArray::<i32>),
        PhysicalDataType::LargeList(_) => ffi_dyn!(array, ListArray::<i64>),
        PhysicalDataType::FixedSizeList(_, _) => ffi_dyn!(array, FixedSizeListArray),
        PhysicalDataType::Struct(_) => ffi_dyn!(array, StructArray),
        PhysicalDataType::Union(_, _, _) => ffi_dyn!(array, UnionArray),
        PhysicalDataType::Dictionary(key_type, _) => {
            with_match_dictionary_key_type!(key_type.as_ref(), |$T| {
                let array = array.as_any().downcast_ref::<DictionaryArray<$T>>().unwrap();
                (
                    array.buffers(),
                    array.children(),
                    Some(array.values().clone()),
                )
            })
        }
    }
}
