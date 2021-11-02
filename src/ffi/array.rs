//! Contains functionality to load an ArrayData from the C Data Interface

use super::ffi::ArrowArrayRef;
use crate::array::{BooleanArray, FromFfi};
use crate::error::{ArrowError, Result};
use crate::{array::*, datatypes::PhysicalType};

/// Reads a valid `ffi` interface into a `Box<dyn Array>`
/// # Errors
/// If and only if:
/// * the data type is not supported
/// * the interface is not valid (e.g. a null pointer)
pub unsafe fn try_from<A: ArrowArrayRef>(array: A) -> Result<Box<dyn Array>> {
    use PhysicalType::*;
    Ok(match array.field().data_type().to_physical_type() {
        Boolean => Box::new(BooleanArray::try_from_ffi(array)?),
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            Box::new(PrimitiveArray::<$T>::try_from_ffi(array)?)
        }),
        Utf8 => Box::new(Utf8Array::<i32>::try_from_ffi(array)?),
        LargeUtf8 => Box::new(Utf8Array::<i64>::try_from_ffi(array)?),
        Binary => Box::new(BinaryArray::<i32>::try_from_ffi(array)?),
        LargeBinary => Box::new(BinaryArray::<i64>::try_from_ffi(array)?),
        FixedSizeBinary => Box::new(FixedSizeBinaryArray::try_from_ffi(array)?),
        List => Box::new(ListArray::<i32>::try_from_ffi(array)?),
        LargeList => Box::new(ListArray::<i64>::try_from_ffi(array)?),
        FixedSizeList => Box::new(FixedSizeListArray::try_from_ffi(array)?),
        Struct => Box::new(StructArray::try_from_ffi(array)?),
        Dictionary(key_type) => {
            with_match_physical_dictionary_key_type!(key_type, |$T| {
                Box::new(DictionaryArray::<$T>::try_from_ffi(array)?)
            })
        }
        Union => Box::new(UnionArray::try_from_ffi(array)?),
        Map => Box::new(MapArray::try_from_ffi(array)?),
        data_type => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Importing PhysicalType \"{:?}\" is not yet supported.",
                data_type
            )))
        }
    })
}
