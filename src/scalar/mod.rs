//! contains the [`Scalar`] trait object representing individual items of [`Array`](crate::array::Array)s,
//! as well as concrete implementations such as [`BooleanScalar`].
use std::any::Any;

use crate::{array::*, datatypes::*, types::NativeType};

mod dictionary;
pub use dictionary::*;
mod equal;
mod primitive;
pub use primitive::*;
mod utf8;
pub use utf8::*;
mod binary;
pub use binary::*;
mod boolean;
pub use boolean::*;
mod list;
pub use list::*;
mod null;
pub use null::*;
mod struct_;
pub use struct_::*;
mod fixed_size_list;
pub use fixed_size_list::*;
mod fixed_size_binary;
pub use fixed_size_binary::*;
mod union;
pub use union::UnionScalar;

/// Trait object declaring an optional value with a [`DataType`].
/// This strait is often used in APIs that accept multiple scalar types.
pub trait Scalar: std::fmt::Debug + Send + Sync + dyn_clone::DynClone + 'static {
    /// convert itself to
    fn as_any(&self) -> &dyn Any;

    /// whether it is valid
    fn is_valid(&self) -> bool;

    /// the logical type.
    fn data_type(&self) -> &DataType;
}

dyn_clone::clone_trait_object!(Scalar);

fn new_primitive<T: NativeType>(array: &PrimitiveArray<T>, index: usize) -> Box<dyn Scalar> {
    let value = if array.is_valid(index) {
        Some(array.value(index))
    } else {
        None
    };
    Box::new(PrimitiveScalar::new(array.data_type().clone(), value))
}

fn new_dictionary<K: DictionaryKey>(array: &DictionaryArray<K>, index: usize) -> Box<dyn Scalar> {
    let value = if array.is_valid(index) {
        Some(array.value(index))
    } else {
        None
    };
    Box::new(DictionaryScalar::<K>::new(array.data_type().clone(), value))
}

fn new_utf8<O: Offset>(array: &Utf8Array<O>, index: usize) -> Box<dyn Scalar> {
    let value = if array.is_valid(index) {
        Some(array.value(index))
    } else {
        None
    };
    Box::new(Utf8Scalar::<O>::new(value))
}

fn new_binary<O: Offset>(array: &BinaryArray<O>, index: usize) -> Box<dyn Scalar> {
    let value = if array.is_valid(index) {
        Some(array.value(index))
    } else {
        None
    };
    Box::new(BinaryScalar::<O>::new(value))
}

fn new_fixed_size_binary(array: &FixedSizeBinaryArray, index: usize) -> Box<dyn Scalar> {
    let value = if array.is_valid(index) {
        Some(array.value(index))
    } else {
        None
    };
    Box::new(FixedSizeBinaryScalar::new(array.data_type().clone(), value))
}

fn new_list<O: Offset>(array: &ListArray<O>, index: usize) -> Box<dyn Scalar> {
    let value = if array.is_valid(index) {
        Some(array.value(index))
    } else {
        None
    };
    Box::new(ListScalar::<O>::new(array.data_type().clone(), value))
}

fn new_fixed_size_list(array: &FixedSizeListArray, index: usize) -> Box<dyn Scalar> {
    let value = if array.is_valid(index) {
        Some(array.value(index))
    } else {
        None
    };
    Box::new(FixedSizeListScalar::new(array.data_type().clone(), value))
}

fn new_boolean(array: &BooleanArray, index: usize) -> Box<dyn Scalar> {
    let value = if array.is_valid(index) {
        Some(array.value(index))
    } else {
        None
    };
    Box::new(BooleanScalar::new(value))
}

fn new_strut(array: &StructArray, index: usize) -> Box<dyn Scalar> {
    let value = if array.is_valid(index) {
        let values = array
            .values()
            .iter()
            .map(|x| new_scalar(x.as_ref(), index))
            .collect();
        Some(values)
    } else {
        None
    };
    Box::new(StructScalar::new(array.data_type().clone(), value))
}

fn new_union(array: &UnionArray, index: usize) -> Box<dyn Scalar> {
    Box::new(UnionScalar::new(
        array.data_type().clone(),
        array.types()[index],
        array.value(index),
    ))
}

/// creates a new [`Scalar`] from an [`Array`].
pub fn new_scalar(array: &dyn Array, index: usize) -> Box<dyn Scalar> {
    use PhysicalType::*;
    match array.data_type().to_physical_type() {
        Null => Box::new(NullScalar::new()),
        Boolean => new_boolean(array.as_any().downcast_ref().unwrap(), index),
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            new_primitive::<$T>(array.as_any().downcast_ref().unwrap(), index)
        }),
        Utf8 => new_utf8::<i32>(array.as_any().downcast_ref().unwrap(), index),
        LargeUtf8 => new_utf8::<i64>(array.as_any().downcast_ref().unwrap(), index),
        Binary => new_binary::<i32>(array.as_any().downcast_ref().unwrap(), index),
        LargeBinary => new_binary::<i64>(array.as_any().downcast_ref().unwrap(), index),
        List => new_list::<i32>(array.as_any().downcast_ref().unwrap(), index),
        LargeList => new_list::<i64>(array.as_any().downcast_ref().unwrap(), index),
        Struct => new_strut(array.as_any().downcast_ref().unwrap(), index),
        FixedSizeBinary => new_fixed_size_binary(array.as_any().downcast_ref().unwrap(), index),
        FixedSizeList => new_fixed_size_list(array.as_any().downcast_ref().unwrap(), index),
        Union => new_union(array.as_any().downcast_ref().unwrap(), index),
        Map => todo!(),
        Dictionary(key_type) => match_integer_type!(key_type, |$T| {
            new_dictionary::<$T>(array.as_any().downcast_ref().unwrap(), index)
        }),
    }
}
