use std::sync::Arc;

use crate::datatypes::PhysicalType;
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
    /// Convert itself from FFI.
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
    use PhysicalType::*;
    match array.data_type().to_physical_type() {
        Null => ffi_dyn!(array, NullArray),
        Boolean => ffi_dyn!(array, BooleanArray),
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            ffi_dyn!(array, PrimitiveArray<$T>)
        }),
        Binary => ffi_dyn!(array, BinaryArray<i32>),
        LargeBinary => ffi_dyn!(array, BinaryArray<i64>),
        FixedSizeBinary => ffi_dyn!(array, FixedSizeBinaryArray),
        Utf8 => ffi_dyn!(array, Utf8Array::<i32>),
        LargeUtf8 => ffi_dyn!(array, Utf8Array::<i64>),
        List => ffi_dyn!(array, ListArray::<i32>),
        LargeList => ffi_dyn!(array, ListArray::<i64>),
        FixedSizeList => ffi_dyn!(array, FixedSizeListArray),
        Struct => ffi_dyn!(array, StructArray),
        Union => ffi_dyn!(array, UnionArray),
        Dictionary(key_type) => {
            with_match_physical_dictionary_key_type!(key_type, |$T| {
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
