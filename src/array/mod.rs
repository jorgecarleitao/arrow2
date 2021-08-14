//! This module contains arrays: fixed-length and immutable containers with optional values
//! that are layed in memory according to the Arrow specification.
//! Each array type has its own `struct`. The following are the main array types:
//!
//! * [`PrimitiveArray`], an array of values with a fixed length such as integers, floats, etc.
//! * [`BooleanArray`], an array of boolean values (stored as a bitmap)
//! * [`Utf8Array`], an array of utf8 values
//! * [`BinaryArray`], an array of binary values
//! * [`ListArray`], an array of arrays (e.g. `[[1, 2], None, [], [None]]`)
//! * [`StructArray`], an array of arrays identified by a string (e.g. `{"a": [1, 2], "b": [true, false]}`)
//!
//! This module contains constructors and accessors to operate on the arrays.
//! All the arrays implement the trait [`Array`] and are often trait objects.
//! Every array has a [`DataType`], which you can access with [`Array::data_type`].
//! This can be used to `downcast_ref` a `&dyn Array` to a concrete struct.
//! Arrays can share memory via [`crate::buffer::Buffer`] and thus cloning and slicing is `O(1)`.
//!
//! This module also contains the mutable counterparts of arrays, that are neither clonable nor slicable, but that
//! can be operated in-place, such as [`MutablePrimitiveArray`] and [`MutableUtf8Array`].
use std::any::Any;
use std::fmt::Display;

use crate::error::Result;
use crate::types::days_ms;
use crate::{
    bitmap::{Bitmap, MutableBitmap},
    datatypes::{DataType, IntervalUnit},
};

/// A trait representing an immutable Arrow array. Arrow arrays are trait objects
/// that are infalibly downcasted to concrete types according to the [`Array::data_type`].
pub trait Array: std::fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    /// The length of the [`Array`]. Every array has a length corresponding to the number of
    /// elements (slots).
    fn len(&self) -> usize;

    /// whether the array is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The [`DataType`] of the [`Array`]. In combination with [`Array::as_any`], this can be
    /// used to downcast trait objects (`dyn Array`) to concrete arrays.
    fn data_type(&self) -> &DataType;

    /// The validity of the [`Array`]: every array has an optional [`Bitmap`] that, when available
    /// specifies whether the array slot is valid or not (null).
    /// When the validity is [`None`], all slots are valid.
    fn validity(&self) -> &Option<Bitmap>;

    /// The number of null slots on this [`Array`]. This is usually used to branch
    /// implementations to cases where optimizations can be made.
    /// # Implementation
    /// This is `O(1)`.
    #[inline]
    fn null_count(&self) -> usize {
        if self.data_type() == &DataType::Null {
            return self.len();
        };
        self.validity()
            .as_ref()
            .map(|x| x.null_count())
            .unwrap_or(0)
    }

    /// Returns whether slot `i` is null.
    /// # Panic
    /// Panics iff `i >= self.len()`.
    #[inline]
    fn is_null(&self, i: usize) -> bool {
        self.validity()
            .as_ref()
            .map(|x| !x.get_bit(i))
            .unwrap_or(false)
    }

    /// Returns whether slot `i` is valid.
    /// # Panic
    /// Panics iff `i >= self.len()`.
    #[inline]
    fn is_valid(&self, i: usize) -> bool {
        !self.is_null(i)
    }

    /// Slices the [`Array`], returning a new `Box<dyn Array>`.
    /// # Implementation
    /// This operation is `O(1)` over `len`, as it amounts to increase two ref counts
    /// and moving the struct to the heap.
    /// # Panic
    /// This function panics iff `offset + length >= self.len()`.
    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array>;
}

/// A trait describing a mutable array; i.e. an array whose values can be changed.
/// Mutable arrays are not `Send + Sync` and cannot be cloned but can be mutated in place,
/// thereby making them useful to perform numeric operations without allocations.
/// As in [`Array`], concrete arrays (such as [`MutablePrimitiveArray`]) implement how they are mutated.
pub trait MutableArray: std::fmt::Debug {
    /// The [`DataType`] of the array.
    fn data_type(&self) -> &DataType;

    /// The length of the array.
    fn len(&self) -> usize;

    /// Whether the array is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The optional validity of the array.
    fn validity(&self) -> &Option<MutableBitmap>;

    /// Convert itself to an (immutable) [`Array`].
    fn as_arc(&mut self) -> Arc<dyn Array>;

    /// Convert to `Any`, to enable dynamic casting.
    fn as_any(&self) -> &dyn Any;

    /// Convert to mutable `Any`, to enable dynamic casting.
    fn as_mut_any(&mut self) -> &mut dyn Any;

    /// Adds a new null element to the array.
    fn push_null(&mut self);

    /// Whether `index` is valid / set.
    /// # Panic
    /// Panics if `index >= self.len()`.
    #[inline]
    fn is_valid(&self, index: usize) -> bool {
        self.validity()
            .as_ref()
            .map(|x| x.get(index))
            .unwrap_or(true)
    }
}

macro_rules! general_dyn {
    ($array:expr, $ty:ty, $f:expr) => {{
        let array = $array.as_any().downcast_ref::<$ty>().unwrap();
        ($f)(array)
    }};
}

macro_rules! fmt_dyn {
    ($array:expr, $ty:ty, $f:expr) => {{
        let mut f = |x: &$ty| x.fmt($f);
        general_dyn!($array, $ty, f)
    }};
}

impl Display for dyn Array {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.data_type() {
            DataType::Null => fmt_dyn!(self, NullArray, f),
            DataType::Boolean => fmt_dyn!(self, BooleanArray, f),
            DataType::Int8 => fmt_dyn!(self, PrimitiveArray<i8>, f),
            DataType::Int16 => fmt_dyn!(self, PrimitiveArray<i16>, f),
            DataType::Int32
            | DataType::Date32
            | DataType::Time32(_)
            | DataType::Interval(IntervalUnit::YearMonth) => {
                fmt_dyn!(self, PrimitiveArray<i32>, f)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                fmt_dyn!(self, PrimitiveArray<days_ms>, f)
            }
            DataType::Int64
            | DataType::Date64
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_) => fmt_dyn!(self, PrimitiveArray<i64>, f),
            DataType::Decimal(_, _) => fmt_dyn!(self, PrimitiveArray<i128>, f),
            DataType::UInt8 => fmt_dyn!(self, PrimitiveArray<u8>, f),
            DataType::UInt16 => fmt_dyn!(self, PrimitiveArray<u16>, f),
            DataType::UInt32 => fmt_dyn!(self, PrimitiveArray<u32>, f),
            DataType::UInt64 => fmt_dyn!(self, PrimitiveArray<u64>, f),
            DataType::Float16 => unreachable!(),
            DataType::Float32 => fmt_dyn!(self, PrimitiveArray<f32>, f),
            DataType::Float64 => fmt_dyn!(self, PrimitiveArray<f64>, f),
            DataType::Binary => fmt_dyn!(self, BinaryArray<i32>, f),
            DataType::LargeBinary => fmt_dyn!(self, BinaryArray<i64>, f),
            DataType::FixedSizeBinary(_) => fmt_dyn!(self, FixedSizeBinaryArray, f),
            DataType::Utf8 => fmt_dyn!(self, Utf8Array::<i32>, f),
            DataType::LargeUtf8 => fmt_dyn!(self, Utf8Array::<i64>, f),
            DataType::List(_) => fmt_dyn!(self, ListArray::<i32>, f),
            DataType::LargeList(_) => fmt_dyn!(self, ListArray::<i64>, f),
            DataType::FixedSizeList(_, _) => fmt_dyn!(self, FixedSizeListArray, f),
            DataType::Struct(_) => fmt_dyn!(self, StructArray, f),
            DataType::Union(_, _, _) => fmt_dyn!(self, UnionArray, f),
            DataType::Dictionary(key_type, _) => match key_type.as_ref() {
                DataType::Int8 => fmt_dyn!(self, DictionaryArray::<i8>, f),
                DataType::Int16 => fmt_dyn!(self, DictionaryArray::<i16>, f),
                DataType::Int32 => fmt_dyn!(self, DictionaryArray::<i32>, f),
                DataType::Int64 => fmt_dyn!(self, DictionaryArray::<i64>, f),
                DataType::UInt8 => fmt_dyn!(self, DictionaryArray::<u8>, f),
                DataType::UInt16 => fmt_dyn!(self, DictionaryArray::<u16>, f),
                DataType::UInt32 => fmt_dyn!(self, DictionaryArray::<u32>, f),
                DataType::UInt64 => fmt_dyn!(self, DictionaryArray::<u64>, f),
                _ => unreachable!(),
            },
        }
    }
}

/// Creates a new [`Array`] with a [`Array::len`] of 0.
pub fn new_empty_array(data_type: DataType) -> Box<dyn Array> {
    match data_type {
        DataType::Null => Box::new(NullArray::new_empty()),
        DataType::Boolean => Box::new(BooleanArray::new_empty()),
        DataType::Int8 => Box::new(PrimitiveArray::<i8>::new_empty(data_type)),
        DataType::Int16 => Box::new(PrimitiveArray::<i16>::new_empty(data_type)),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            Box::new(PrimitiveArray::<i32>::new_empty(data_type))
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            Box::new(PrimitiveArray::<days_ms>::new_empty(data_type))
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => Box::new(PrimitiveArray::<i64>::new_empty(data_type)),
        DataType::Decimal(_, _) => Box::new(PrimitiveArray::<i128>::new_empty(data_type)),
        DataType::UInt8 => Box::new(PrimitiveArray::<u8>::new_empty(data_type)),
        DataType::UInt16 => Box::new(PrimitiveArray::<u16>::new_empty(data_type)),
        DataType::UInt32 => Box::new(PrimitiveArray::<u32>::new_empty(data_type)),
        DataType::UInt64 => Box::new(PrimitiveArray::<u64>::new_empty(data_type)),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => Box::new(PrimitiveArray::<f32>::new_empty(data_type)),
        DataType::Float64 => Box::new(PrimitiveArray::<f64>::new_empty(data_type)),
        DataType::Binary => Box::new(BinaryArray::<i32>::new_empty()),
        DataType::LargeBinary => Box::new(BinaryArray::<i64>::new_empty()),
        DataType::FixedSizeBinary(_) => Box::new(FixedSizeBinaryArray::new_empty(data_type)),
        DataType::Utf8 => Box::new(Utf8Array::<i32>::new_empty()),
        DataType::LargeUtf8 => Box::new(Utf8Array::<i64>::new_empty()),
        DataType::List(_) => Box::new(ListArray::<i32>::new_empty(data_type)),
        DataType::LargeList(_) => Box::new(ListArray::<i64>::new_empty(data_type)),
        DataType::FixedSizeList(_, _) => Box::new(FixedSizeListArray::new_empty(data_type)),
        DataType::Struct(fields) => Box::new(StructArray::new_empty(&fields)),
        DataType::Union(_, _, _) => Box::new(UnionArray::new_empty(data_type)),
        DataType::Dictionary(key_type, value_type) => match key_type.as_ref() {
            DataType::Int8 => Box::new(DictionaryArray::<i8>::new_empty(*value_type)),
            DataType::Int16 => Box::new(DictionaryArray::<i16>::new_empty(*value_type)),
            DataType::Int32 => Box::new(DictionaryArray::<i32>::new_empty(*value_type)),
            DataType::Int64 => Box::new(DictionaryArray::<i64>::new_empty(*value_type)),
            DataType::UInt8 => Box::new(DictionaryArray::<u8>::new_empty(*value_type)),
            DataType::UInt16 => Box::new(DictionaryArray::<u16>::new_empty(*value_type)),
            DataType::UInt32 => Box::new(DictionaryArray::<u32>::new_empty(*value_type)),
            DataType::UInt64 => Box::new(DictionaryArray::<u64>::new_empty(*value_type)),
            _ => unreachable!(),
        },
    }
}

/// Creates a new [`Array`] of [`DataType`] `data_type` and `length`.
/// The array is guaranteed to have [`Array::null_count`] equal to [`Array::len`].
pub fn new_null_array(data_type: DataType, length: usize) -> Box<dyn Array> {
    match data_type {
        DataType::Null => Box::new(NullArray::new_null(length)),
        DataType::Boolean => Box::new(BooleanArray::new_null(length)),
        DataType::Int8 => Box::new(PrimitiveArray::<i8>::new_null(data_type, length)),
        DataType::Int16 => Box::new(PrimitiveArray::<i16>::new_null(data_type, length)),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            Box::new(PrimitiveArray::<i32>::new_null(data_type, length))
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            Box::new(PrimitiveArray::<days_ms>::new_null(data_type, length))
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => Box::new(PrimitiveArray::<i64>::new_null(data_type, length)),
        DataType::Decimal(_, _) => Box::new(PrimitiveArray::<i128>::new_null(data_type, length)),
        DataType::UInt8 => Box::new(PrimitiveArray::<u8>::new_null(data_type, length)),
        DataType::UInt16 => Box::new(PrimitiveArray::<u16>::new_null(data_type, length)),
        DataType::UInt32 => Box::new(PrimitiveArray::<u32>::new_null(data_type, length)),
        DataType::UInt64 => Box::new(PrimitiveArray::<u64>::new_null(data_type, length)),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => Box::new(PrimitiveArray::<f32>::new_null(data_type, length)),
        DataType::Float64 => Box::new(PrimitiveArray::<f64>::new_null(data_type, length)),
        DataType::Binary => Box::new(BinaryArray::<i32>::new_null(length)),
        DataType::LargeBinary => Box::new(BinaryArray::<i64>::new_null(length)),
        DataType::FixedSizeBinary(_) => Box::new(FixedSizeBinaryArray::new_null(data_type, length)),
        DataType::Utf8 => Box::new(Utf8Array::<i32>::new_null(length)),
        DataType::LargeUtf8 => Box::new(Utf8Array::<i64>::new_null(length)),
        DataType::List(_) => Box::new(ListArray::<i32>::new_null(data_type, length)),
        DataType::LargeList(_) => Box::new(ListArray::<i64>::new_null(data_type, length)),
        DataType::FixedSizeList(_, _) => Box::new(FixedSizeListArray::new_null(data_type, length)),
        DataType::Struct(fields) => Box::new(StructArray::new_null(&fields, length)),
        DataType::Union(_, _, _) => Box::new(UnionArray::new_null(data_type, length)),
        DataType::Dictionary(key_type, value_type) => match key_type.as_ref() {
            DataType::Int8 => Box::new(DictionaryArray::<i8>::new_null(*value_type, length)),
            DataType::Int16 => Box::new(DictionaryArray::<i16>::new_null(*value_type, length)),
            DataType::Int32 => Box::new(DictionaryArray::<i32>::new_null(*value_type, length)),
            DataType::Int64 => Box::new(DictionaryArray::<i64>::new_null(*value_type, length)),
            DataType::UInt8 => Box::new(DictionaryArray::<u8>::new_null(*value_type, length)),
            DataType::UInt16 => Box::new(DictionaryArray::<u16>::new_null(*value_type, length)),
            DataType::UInt32 => Box::new(DictionaryArray::<u32>::new_null(*value_type, length)),
            DataType::UInt64 => Box::new(DictionaryArray::<u64>::new_null(*value_type, length)),
            _ => unreachable!(),
        },
    }
}

macro_rules! clone_dyn {
    ($array:expr, $ty:ty) => {{
        let f = |x: &$ty| Box::new(x.clone());
        general_dyn!($array, $ty, f)
    }};
}

/// Clones a dynamic [`Array`].
/// # Implementation
/// This operation is `O(1)` over `len`, as it amounts to increase two ref counts
/// and moving the concrete struct under a `Box`.
pub fn clone(array: &dyn Array) -> Box<dyn Array> {
    match array.data_type() {
        DataType::Null => clone_dyn!(array, NullArray),
        DataType::Boolean => clone_dyn!(array, BooleanArray),
        DataType::Int8 => clone_dyn!(array, PrimitiveArray<i8>),
        DataType::Int16 => clone_dyn!(array, PrimitiveArray<i16>),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            clone_dyn!(array, PrimitiveArray<i32>)
        }
        DataType::Interval(IntervalUnit::DayTime) => clone_dyn!(array, PrimitiveArray<days_ms>),
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => clone_dyn!(array, PrimitiveArray<i64>),
        DataType::Decimal(_, _) => clone_dyn!(array, PrimitiveArray<i128>),
        DataType::UInt8 => clone_dyn!(array, PrimitiveArray<u8>),
        DataType::UInt16 => clone_dyn!(array, PrimitiveArray<u16>),
        DataType::UInt32 => clone_dyn!(array, PrimitiveArray<u32>),
        DataType::UInt64 => clone_dyn!(array, PrimitiveArray<u64>),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => clone_dyn!(array, PrimitiveArray<f32>),
        DataType::Float64 => clone_dyn!(array, PrimitiveArray<f64>),
        DataType::Binary => clone_dyn!(array, BinaryArray<i32>),
        DataType::LargeBinary => clone_dyn!(array, BinaryArray<i64>),
        DataType::FixedSizeBinary(_) => clone_dyn!(array, FixedSizeBinaryArray),
        DataType::Utf8 => clone_dyn!(array, Utf8Array::<i32>),
        DataType::LargeUtf8 => clone_dyn!(array, Utf8Array::<i64>),
        DataType::List(_) => clone_dyn!(array, ListArray::<i32>),
        DataType::LargeList(_) => clone_dyn!(array, ListArray::<i64>),
        DataType::FixedSizeList(_, _) => clone_dyn!(array, FixedSizeListArray),
        DataType::Struct(_) => clone_dyn!(array, StructArray),
        DataType::Union(_, _, _) => clone_dyn!(array, UnionArray),
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::Int8 => clone_dyn!(array, DictionaryArray::<i8>),
            DataType::Int16 => clone_dyn!(array, DictionaryArray::<i16>),
            DataType::Int32 => clone_dyn!(array, DictionaryArray::<i32>),
            DataType::Int64 => clone_dyn!(array, DictionaryArray::<i64>),
            DataType::UInt8 => clone_dyn!(array, DictionaryArray::<u8>),
            DataType::UInt16 => clone_dyn!(array, DictionaryArray::<u16>),
            DataType::UInt32 => clone_dyn!(array, DictionaryArray::<u32>),
            DataType::UInt64 => clone_dyn!(array, DictionaryArray::<u64>),
            _ => unreachable!(),
        },
    }
}

mod binary;
mod boolean;
mod dictionary;
mod display;
mod fixed_size_binary;
mod fixed_size_list;
mod list;
mod null;
mod primitive;
mod specification;
mod struct_;
mod union;
mod utf8;

mod equal;
mod ffi;
pub mod growable;
pub mod ord;

pub use display::get_display;

pub use binary::{BinaryArray, MutableBinaryArray};
pub use boolean::{BooleanArray, MutableBooleanArray};
pub use dictionary::{DictionaryArray, DictionaryKey, MutableDictionaryArray};
pub use fixed_size_binary::FixedSizeBinaryArray;
pub use fixed_size_list::FixedSizeListArray;
pub use list::{ListArray, MutableListArray};
pub use null::NullArray;
pub use primitive::*;
pub use specification::Offset;
pub use struct_::StructArray;
pub use union::UnionArray;
pub use utf8::{MutableUtf8Array, Utf8Array, Utf8ValuesIter};

pub(crate) use self::ffi::buffers_children_dictionary;
pub use self::ffi::FromFfi;
pub use self::ffi::ToFfi;

/// A trait describing the ability of a struct to create itself from a iterator.
/// This is similar to [`Extend`], but accepted the creation to error.
pub trait TryExtend<A> {
    fn try_extend<I: IntoIterator<Item = A>>(&mut self, iter: I) -> Result<()>;
}

fn display_helper<T: std::fmt::Display, I: IntoIterator<Item = Option<T>>>(iter: I) -> Vec<String> {
    let iterator = iter.into_iter();
    let len = iterator.size_hint().0;
    if len <= 100 {
        iterator
            .map(|x| match x {
                Some(x) => x.to_string(),
                None => "".to_string(),
            })
            .collect::<Vec<_>>()
    } else {
        iterator
            .enumerate()
            .filter_map(|(i, x)| {
                if i == 5 {
                    Some(format!("...({})...", len - 10))
                } else if i < 5 || i > (len - 5) {
                    Some(match x {
                        Some(x) => x.to_string(),
                        None => "".to_string(),
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }
}

fn display_fmt<T: std::fmt::Display, I: IntoIterator<Item = Option<T>>>(
    iter: I,
    head: &str,
    f: &mut std::fmt::Formatter<'_>,
    new_lines: bool,
) -> std::fmt::Result {
    let result = display_helper(iter);
    if new_lines {
        write!(f, "{}[\n{}\n]", head, result.join(",\n"))
    } else {
        write!(f, "{}[{}]", head, result.join(", "))
    }
}

/// Trait that list arrays implement for the purposes of DRY.
pub trait IterableListArray: Array {
    fn value(&self, i: usize) -> Box<dyn Array>;
}

/// Trait that [`BinaryArray`] and [`Utf8Array`] implement for the purposes of DRY.
/// # Safety
/// The implementer must ensure that
/// 1. `offsets.len() > 0`
/// 2. `offsets[i] >= offsets[i-1] for all i`
/// 3. `offsets[i] < values.len() for all i`
pub unsafe trait GenericBinaryArray<O: Offset>: Array {
    fn values(&self) -> &[u8];
    fn offsets(&self) -> &[O];
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::*;

    #[test]
    fn nulls() {
        let datatypes = vec![
            DataType::Int32,
            DataType::Float64,
            DataType::Utf8,
            DataType::Binary,
            DataType::List(Box::new(Field::new("a", DataType::Binary, true))),
        ];
        let a = datatypes
            .into_iter()
            .all(|x| new_null_array(x, 10).null_count() == 10);
        assert!(a);
    }

    #[test]
    fn empty() {
        let datatypes = vec![
            DataType::Int32,
            DataType::Float64,
            DataType::Utf8,
            DataType::Binary,
            DataType::List(Box::new(Field::new("a", DataType::Binary, true))),
            DataType::Union(vec![Field::new("a", DataType::Binary, true)], None, true),
            DataType::Union(vec![Field::new("a", DataType::Binary, true)], None, false),
        ];
        let a = datatypes.into_iter().all(|x| new_empty_array(x).len() == 0);
        assert!(a);
    }

    #[test]
    fn test_clone() {
        let datatypes = vec![
            DataType::Int32,
            DataType::Float64,
            DataType::Utf8,
            DataType::Binary,
            DataType::List(Box::new(Field::new("a", DataType::Binary, true))),
        ];
        let a = datatypes
            .into_iter()
            .all(|x| clone(new_null_array(x.clone(), 10).as_ref()) == new_null_array(x, 10));
        assert!(a);
    }
}

// backward compatibility
use std::sync::Arc;
pub type ArrayRef = Arc<dyn Array>;
