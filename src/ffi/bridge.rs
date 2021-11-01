use std::sync::Arc;

use crate::array::*;

macro_rules! ffi_dyn {
    ($array:expr, $ty:ty) => {{
        let a = $array.as_any().downcast_ref::<$ty>().unwrap();
        if a.offset().is_some() {
            $array
        } else {
            Arc::new(a.to_ffi_aligned())
        }
    }};
}

pub fn align_to_c_data_interface(array: Arc<dyn Array>) -> Arc<dyn Array> {
    use crate::datatypes::PhysicalType::*;
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
        Map => ffi_dyn!(array, MapArray),
        Dictionary(key_type) => {
            with_match_physical_dictionary_key_type!(key_type, |$T| {
                ffi_dyn!(array, DictionaryArray<$T>)
            })
        }
    }
}
