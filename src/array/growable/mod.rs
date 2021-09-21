//! Contains the trait [`Growable`] and corresponding concreate implementations, one per concrete array,
//! that offer the ability to create a new [`Array`] out of slices of existing [`Array`]s.

use crate::array::*;
use crate::datatypes::*;

mod binary;
pub use binary::GrowableBinary;
mod boolean;
pub use boolean::GrowableBoolean;
mod fixed_binary;
pub use fixed_binary::GrowableFixedSizeBinary;
mod null;
pub use null::GrowableNull;
mod primitive;
pub use primitive::GrowablePrimitive;
mod list;
pub use list::GrowableList;
mod structure;
pub use structure::GrowableStruct;
mod utf8;
pub use utf8::GrowableUtf8;
mod dictionary;
pub use dictionary::GrowableDictionary;

mod utils;

/// Describes a struct that can be extended from slices of other pre-existing [`Array`]s.
/// This is used in operations where a new array is built out of other arrays such
/// as filter and concatenation.
pub trait Growable<'a> {
    /// Extends this [`Growable`] with elements from the bounded [`Array`] at index `index` from
    /// a slice starting at `start` and length `len`.
    /// # Panic
    /// This function panics if the range is out of bounds, i.e. if `start + len >= array.len()`.
    fn extend(&mut self, index: usize, start: usize, len: usize);

    /// Extends this [`Growable`] with null elements, disregarding the bound arrays
    fn extend_validity(&mut self, additional: usize);

    /// Converts this [`Growable`] to an [`Arc<dyn Array>`], thereby finishing the mutation.
    /// Self will be empty after such operation.
    fn as_arc(&mut self) -> std::sync::Arc<dyn Array> {
        self.as_box().into()
    }

    /// Converts this [`Growable`] to an [`Box<dyn Array>`], thereby finishing the mutation.
    /// Self will be empty after such operation
    fn as_box(&mut self) -> Box<dyn Array>;
}

macro_rules! dyn_growable {
    ($ty:ty, $arrays:expr, $use_validity:expr, $capacity:expr) => {{
        let arrays = $arrays
            .iter()
            .map(|array| array.as_any().downcast_ref().unwrap())
            .collect::<Vec<_>>();
        Box::new(primitive::GrowablePrimitive::<$ty>::new(
            arrays,
            $use_validity,
            $capacity,
        ))
    }};
}

macro_rules! dyn_dict_growable {
    ($ty:ty, $arrays:expr, $use_validity:expr, $capacity:expr) => {{
        let arrays = $arrays
            .iter()
            .map(|array| {
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<$ty>>()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        Box::new(dictionary::GrowableDictionary::<$ty>::new(
            &arrays,
            $use_validity,
            $capacity,
        ))
    }};
}

/// Creates a new [`Growable`] from an arbitrary number of [`Array`]s.
/// # Panics
/// This function panics iff
/// * the arrays do not have the same [`DataType`].
/// * `arrays.is_empty()`.
pub fn make_growable<'a>(
    arrays: &[&'a dyn Array],
    use_validity: bool,
    capacity: usize,
) -> Box<dyn Growable<'a> + 'a> {
    assert!(!arrays.is_empty());
    let data_type = arrays[0].data_type();
    assert!(arrays.iter().all(|&item| item.data_type() == data_type));

    use PhysicalType::*;
    match data_type.to_physical_type() {
        Null => Box::new(null::GrowableNull::new(data_type.clone())),
        Boolean => {
            let arrays = arrays
                .iter()
                .map(|array| array.as_any().downcast_ref().unwrap())
                .collect::<Vec<_>>();
            Box::new(boolean::GrowableBoolean::new(
                arrays,
                use_validity,
                capacity,
            ))
        }
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            dyn_growable!($T, arrays, use_validity, capacity)
        }),
        Utf8 => {
            let arrays = arrays
                .iter()
                .map(|array| array.as_any().downcast_ref().unwrap())
                .collect::<Vec<_>>();
            Box::new(utf8::GrowableUtf8::<i32>::new(
                arrays,
                use_validity,
                capacity,
            ))
        }
        LargeUtf8 => {
            let arrays = arrays
                .iter()
                .map(|array| array.as_any().downcast_ref().unwrap())
                .collect::<Vec<_>>();
            Box::new(utf8::GrowableUtf8::<i64>::new(
                arrays,
                use_validity,
                capacity,
            ))
        }
        Binary => {
            let arrays = arrays
                .iter()
                .map(|array| array.as_any().downcast_ref().unwrap())
                .collect::<Vec<_>>();
            Box::new(binary::GrowableBinary::<i32>::new(
                arrays,
                use_validity,
                capacity,
            ))
        }
        LargeBinary => {
            let arrays = arrays
                .iter()
                .map(|array| array.as_any().downcast_ref().unwrap())
                .collect::<Vec<_>>();
            Box::new(binary::GrowableBinary::<i64>::new(
                arrays,
                use_validity,
                capacity,
            ))
        }
        FixedSizeBinary => {
            let arrays = arrays
                .iter()
                .map(|array| array.as_any().downcast_ref().unwrap())
                .collect::<Vec<_>>();
            Box::new(fixed_binary::GrowableFixedSizeBinary::new(
                arrays,
                use_validity,
                capacity,
            ))
        }
        List => {
            let arrays = arrays
                .iter()
                .map(|array| array.as_any().downcast_ref().unwrap())
                .collect::<Vec<_>>();
            Box::new(list::GrowableList::<i32>::new(
                arrays,
                use_validity,
                capacity,
            ))
        }
        LargeList => {
            let arrays = arrays
                .iter()
                .map(|array| array.as_any().downcast_ref().unwrap())
                .collect::<Vec<_>>();
            Box::new(list::GrowableList::<i64>::new(
                arrays,
                use_validity,
                capacity,
            ))
        }
        Struct => {
            let arrays = arrays
                .iter()
                .map(|array| array.as_any().downcast_ref().unwrap())
                .collect::<Vec<_>>();
            Box::new(structure::GrowableStruct::new(
                arrays,
                use_validity,
                capacity,
            ))
        }
        FixedSizeList => todo!(),
        Union => todo!(),
        Dictionary(key_type) => {
            with_match_physical_dictionary_key_type!(key_type, |$T| {
                dyn_dict_growable!($T, arrays, use_validity, capacity)
            })
        }
    }
}
