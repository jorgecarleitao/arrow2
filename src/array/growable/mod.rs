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

/// A trait describing a struct that can be extended from slices of pre-existing [`Array`]s.
/// This is used in operations where a new array is built out of other arrays such,
/// as filtering and concatenation.
pub trait Growable<'a> {
    /// Extends this [`Growable`] with elements from the bounded [`Array`] at index `index` from
    /// a slice starting at `start` and length `len`.
    /// # Panic
    /// This function panics if the range is out of bounds, i.e. if `start + len >= array.len()`.
    fn extend(&mut self, index: usize, start: usize, len: usize);

    /// Extends this [`Growable`] with null elements, disregarding the bound arrays
    fn extend_validity(&mut self, additional: usize);

    /// Converts itself to an `Arc<dyn Array>`, thereby finishing the mutation.
    /// Self will be empty after such operation
    fn as_arc(&mut self) -> std::sync::Arc<dyn Array> {
        self.as_box().into()
    }

    /// Converts itself to an `Box<dyn Array>`, thereby finishing the mutation.
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

/// Creates a new [`Growable`] from an arbitrary number of dynamic [`Array`]s.
/// # Panics
/// This function panics iff
/// * the arrays do not have the same [`DataType`].
/// * `arrays.is_empty`.
pub fn make_growable<'a>(
    arrays: &[&'a dyn Array],
    use_validity: bool,
    capacity: usize,
) -> Box<dyn Growable<'a> + 'a> {
    assert!(!arrays.is_empty());
    let data_type = arrays[0].data_type();
    assert!(arrays.iter().all(|&item| item.data_type() == data_type));

    match data_type {
        DataType::Null => Box::new(null::GrowableNull::new()),
        DataType::Boolean => {
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
        DataType::Int8 => dyn_growable!(i8, arrays, use_validity, capacity),
        DataType::Int16 => dyn_growable!(i16, arrays, use_validity, capacity),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            dyn_growable!(i32, arrays, use_validity, capacity)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => {
            dyn_growable!(i64, arrays, use_validity, capacity)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            dyn_growable!(days_ms, arrays, use_validity, capacity)
        }
        DataType::Decimal(_, _) => dyn_growable!(i128, arrays, use_validity, capacity),
        DataType::UInt8 => dyn_growable!(u8, arrays, use_validity, capacity),
        DataType::UInt16 => dyn_growable!(u16, arrays, use_validity, capacity),
        DataType::UInt32 => dyn_growable!(u32, arrays, use_validity, capacity),
        DataType::UInt64 => dyn_growable!(u64, arrays, use_validity, capacity),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => dyn_growable!(f32, arrays, use_validity, capacity),
        DataType::Float64 => dyn_growable!(f64, arrays, use_validity, capacity),
        DataType::Utf8 => {
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
        DataType::LargeUtf8 => {
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
        DataType::Binary => {
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
        DataType::LargeBinary => {
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
        DataType::FixedSizeBinary(_) => {
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

        DataType::List(_) => {
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
        DataType::LargeList(_) => {
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
        DataType::Struct(_) => {
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
        DataType::FixedSizeList(_, _) => todo!(),
        DataType::Union(_, _, _) => todo!(),
        DataType::Dictionary(key_type, _) => {
            with_match_dictionary_key_type!(key_type.as_ref(), |$T| {
                dyn_dict_growable!($T, arrays, use_validity, capacity)
            })
        }
        DataType::Extension(_, _, _) => todo!(),
    }
}
