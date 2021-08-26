//! fixed-length and immutable containers with optional values
//! that are layed in memory according to the Arrow specification.
//! Each array type has its own `struct`. The following are the main array types:
//! * [`PrimitiveArray`], an array of values with a fixed length such as integers, floats, etc.
//! * [`BooleanArray`], an array of boolean values (stored as a bitmap)
//! * [`Utf8Array`], an array of utf8 values
//! * [`BinaryArray`], an array of binary values
//! * [`ListArray`], an array of arrays (e.g. `[[1, 2], None, [], [None]]`)
//! * [`StructArray`], an array of arrays identified by a string (e.g. `{"a": [1, 2], "b": [true, false]}`)
//! All arrays implement the trait [`Array`] and are often trait objects that can be downcasted
//! to a concrete struct based on [`DataType`] available from [`Array::data_type`].
//! Arrays share memory via [`crate::buffer::Buffer`] and thus cloning and slicing them `O(1)`.
//!
//! This module also contains the mutable counterparts of arrays, that are neither clonable nor slicable, but that
//! can be operated in-place, such as [`MutablePrimitiveArray`] and [`MutableUtf8Array`].
use std::any::Any;
use std::fmt::Display;

use crate::bitmap::{Bitmap, MutableBitmap};
use crate::datatypes::{DataType, PhysicalDataType};
use crate::error::Result;
use crate::types::days_ms;

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

macro_rules! with_match_dictionary_key_type {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    match $key_type {
        DataType::Int8 => __with_ty__! { i8 },
        DataType::Int16 => __with_ty__! { i16 },
        DataType::Int32 => __with_ty__! { i32 },
        DataType::Int64 => __with_ty__! { i64 },
        DataType::UInt8 => __with_ty__! { u8 },
        DataType::UInt16 => __with_ty__! { u16 },
        DataType::UInt32 => __with_ty__! { u32 },
        DataType::UInt64 => __with_ty__! { u64 },
        _ => ::core::unreachable!("A dictionary key type can only be of integer types"),
    }
})}

impl Display for dyn Array {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let physical_type = self.data_type().to_physical_type();
        match physical_type {
            PhysicalDataType::Null => fmt_dyn!(self, NullArray, f),
            PhysicalDataType::Boolean => fmt_dyn!(self, BooleanArray, f),
            PhysicalDataType::Int8 => fmt_dyn!(self, PrimitiveArray<i8>, f),
            PhysicalDataType::Int16 => fmt_dyn!(self, PrimitiveArray<i16>, f),
            PhysicalDataType::Int32 => {
                fmt_dyn!(self, PrimitiveArray<i32>, f)
            }
            PhysicalDataType::DaysMs => {
                fmt_dyn!(self, PrimitiveArray<days_ms>, f)
            }
            PhysicalDataType::Int64 => fmt_dyn!(self, PrimitiveArray<i64>, f),
            PhysicalDataType::Int128 => fmt_dyn!(self, PrimitiveArray<i128>, f),
            PhysicalDataType::UInt8 => fmt_dyn!(self, PrimitiveArray<u8>, f),
            PhysicalDataType::UInt16 => fmt_dyn!(self, PrimitiveArray<u16>, f),
            PhysicalDataType::UInt32 => fmt_dyn!(self, PrimitiveArray<u32>, f),
            PhysicalDataType::UInt64 => fmt_dyn!(self, PrimitiveArray<u64>, f),
            PhysicalDataType::Float16 => unreachable!(),
            PhysicalDataType::Float32 => fmt_dyn!(self, PrimitiveArray<f32>, f),
            PhysicalDataType::Float64 => fmt_dyn!(self, PrimitiveArray<f64>, f),
            PhysicalDataType::Binary => fmt_dyn!(self, BinaryArray<i32>, f),
            PhysicalDataType::LargeBinary => fmt_dyn!(self, BinaryArray<i64>, f),
            PhysicalDataType::FixedSizeBinary(_) => fmt_dyn!(self, FixedSizeBinaryArray, f),
            PhysicalDataType::Utf8 => fmt_dyn!(self, Utf8Array::<i32>, f),
            PhysicalDataType::LargeUtf8 => fmt_dyn!(self, Utf8Array::<i64>, f),
            PhysicalDataType::List(_) => fmt_dyn!(self, ListArray::<i32>, f),
            PhysicalDataType::LargeList(_) => fmt_dyn!(self, ListArray::<i64>, f),
            PhysicalDataType::FixedSizeList(_, _) => fmt_dyn!(self, FixedSizeListArray, f),
            PhysicalDataType::Struct(_) => fmt_dyn!(self, StructArray, f),
            PhysicalDataType::Union(_, _, _) => fmt_dyn!(self, UnionArray, f),
            PhysicalDataType::Dictionary(key_type, _) => {
                with_match_dictionary_key_type!(key_type.as_ref(), |$T| {
                    fmt_dyn!(self, DictionaryArray::<$T>, f)
                })
            }
        }
    }
}

/// Creates a new [`Array`] with a [`Array::len`] of 0.
pub fn new_empty_array(data_type: DataType) -> Box<dyn Array> {
    let physical_type = data_type.to_physical_type();
    match physical_type {
        PhysicalDataType::Null => Box::new(NullArray::new_empty()),
        PhysicalDataType::Boolean => Box::new(BooleanArray::new_empty(data_type)),
        PhysicalDataType::Int8 => Box::new(PrimitiveArray::<i8>::new_empty(data_type)),
        PhysicalDataType::Int16 => Box::new(PrimitiveArray::<i16>::new_empty(data_type)),
        PhysicalDataType::Int32 => Box::new(PrimitiveArray::<i32>::new_empty(data_type)),
        PhysicalDataType::DaysMs => Box::new(PrimitiveArray::<days_ms>::new_empty(data_type)),
        PhysicalDataType::Int64 => Box::new(PrimitiveArray::<i64>::new_empty(data_type)),
        PhysicalDataType::Int128 => Box::new(PrimitiveArray::<i128>::new_empty(data_type)),
        PhysicalDataType::UInt8 => Box::new(PrimitiveArray::<u8>::new_empty(data_type)),
        PhysicalDataType::UInt16 => Box::new(PrimitiveArray::<u16>::new_empty(data_type)),
        PhysicalDataType::UInt32 => Box::new(PrimitiveArray::<u32>::new_empty(data_type)),
        PhysicalDataType::UInt64 => Box::new(PrimitiveArray::<u64>::new_empty(data_type)),
        PhysicalDataType::Float16 => unreachable!(),
        PhysicalDataType::Float32 => Box::new(PrimitiveArray::<f32>::new_empty(data_type)),
        PhysicalDataType::Float64 => Box::new(PrimitiveArray::<f64>::new_empty(data_type)),
        PhysicalDataType::Binary => Box::new(BinaryArray::<i32>::new_empty()),
        PhysicalDataType::LargeBinary => Box::new(BinaryArray::<i64>::new_empty()),
        PhysicalDataType::FixedSizeBinary(_) => {
            Box::new(FixedSizeBinaryArray::new_empty(data_type))
        }
        PhysicalDataType::Utf8 => Box::new(Utf8Array::<i32>::new_empty()),
        PhysicalDataType::LargeUtf8 => Box::new(Utf8Array::<i64>::new_empty()),
        PhysicalDataType::List(_) => Box::new(ListArray::<i32>::new_empty(data_type)),
        PhysicalDataType::LargeList(_) => Box::new(ListArray::<i64>::new_empty(data_type)),
        PhysicalDataType::FixedSizeList(_, _) => Box::new(FixedSizeListArray::new_empty(data_type)),
        PhysicalDataType::Struct(fields) => Box::new(StructArray::new_empty(&fields)),
        PhysicalDataType::Union(_, _, _) => Box::new(UnionArray::new_empty(data_type)),
        PhysicalDataType::Dictionary(key_type, value_type) => {
            with_match_dictionary_key_type!(key_type.as_ref(), |$T| {
                Box::new(DictionaryArray::<$T>::new_empty(*value_type))
            })
        }
    }
}

/// Creates a new [`Array`] of [`DataType`] `data_type` and `length`.
/// The array is guaranteed to have [`Array::null_count`] equal to [`Array::len`]
/// for all types except Union, which does not have a validity.
pub fn new_null_array(data_type: DataType, length: usize) -> Box<dyn Array> {
    let physical_type = data_type.to_physical_type();
    match physical_type {
        PhysicalDataType::Null => Box::new(NullArray::new_null(length)),
        PhysicalDataType::Boolean => Box::new(BooleanArray::new_null(data_type, length)),
        PhysicalDataType::Int8 => Box::new(PrimitiveArray::<i8>::new_null(data_type, length)),
        PhysicalDataType::Int16 => Box::new(PrimitiveArray::<i16>::new_null(data_type, length)),
        PhysicalDataType::Int32 => Box::new(PrimitiveArray::<i32>::new_null(data_type, length)),
        PhysicalDataType::DaysMs => {
            Box::new(PrimitiveArray::<days_ms>::new_null(data_type, length))
        }
        PhysicalDataType::Int64 => Box::new(PrimitiveArray::<i64>::new_null(data_type, length)),
        PhysicalDataType::Int128 => Box::new(PrimitiveArray::<i128>::new_null(data_type, length)),
        PhysicalDataType::UInt8 => Box::new(PrimitiveArray::<u8>::new_null(data_type, length)),
        PhysicalDataType::UInt16 => Box::new(PrimitiveArray::<u16>::new_null(data_type, length)),
        PhysicalDataType::UInt32 => Box::new(PrimitiveArray::<u32>::new_null(data_type, length)),
        PhysicalDataType::UInt64 => Box::new(PrimitiveArray::<u64>::new_null(data_type, length)),
        PhysicalDataType::Float16 => unreachable!(),
        PhysicalDataType::Float32 => Box::new(PrimitiveArray::<f32>::new_null(data_type, length)),
        PhysicalDataType::Float64 => Box::new(PrimitiveArray::<f64>::new_null(data_type, length)),
        PhysicalDataType::Binary => Box::new(BinaryArray::<i32>::new_null(length)),
        PhysicalDataType::LargeBinary => Box::new(BinaryArray::<i64>::new_null(length)),
        PhysicalDataType::FixedSizeBinary(_) => {
            Box::new(FixedSizeBinaryArray::new_null(data_type, length))
        }
        PhysicalDataType::Utf8 => Box::new(Utf8Array::<i32>::new_null(length)),
        PhysicalDataType::LargeUtf8 => Box::new(Utf8Array::<i64>::new_null(length)),
        PhysicalDataType::List(_) => Box::new(ListArray::<i32>::new_null(data_type, length)),
        PhysicalDataType::LargeList(_) => Box::new(ListArray::<i64>::new_null(data_type, length)),
        PhysicalDataType::FixedSizeList(_, _) => {
            Box::new(FixedSizeListArray::new_null(data_type, length))
        }
        PhysicalDataType::Struct(fields) => Box::new(StructArray::new_null(&fields, length)),
        PhysicalDataType::Union(_, _, _) => Box::new(UnionArray::new_null(data_type, length)),
        PhysicalDataType::Dictionary(key_type, value_type) => {
            with_match_dictionary_key_type!(key_type.as_ref(), |$T| {
                Box::new(DictionaryArray::<$T>::new_null(*value_type, length))
            })
        }
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
    let physical_type = array.data_type().to_physical_type();
    match physical_type {
        PhysicalDataType::Null => clone_dyn!(array, NullArray),
        PhysicalDataType::Boolean => clone_dyn!(array, BooleanArray),
        PhysicalDataType::Int8 => clone_dyn!(array, PrimitiveArray<i8>),
        PhysicalDataType::Int16 => clone_dyn!(array, PrimitiveArray<i16>),
        PhysicalDataType::Int32 => {
            clone_dyn!(array, PrimitiveArray<i32>)
        }
        PhysicalDataType::DaysMs => {
            clone_dyn!(array, PrimitiveArray<days_ms>)
        }
        PhysicalDataType::Int64 => clone_dyn!(array, PrimitiveArray<i64>),
        PhysicalDataType::Int128 => clone_dyn!(array, PrimitiveArray<i128>),
        PhysicalDataType::UInt8 => clone_dyn!(array, PrimitiveArray<u8>),
        PhysicalDataType::UInt16 => clone_dyn!(array, PrimitiveArray<u16>),
        PhysicalDataType::UInt32 => clone_dyn!(array, PrimitiveArray<u32>),
        PhysicalDataType::UInt64 => clone_dyn!(array, PrimitiveArray<u64>),
        PhysicalDataType::Float16 => unreachable!(),
        PhysicalDataType::Float32 => clone_dyn!(array, PrimitiveArray<f32>),
        PhysicalDataType::Float64 => clone_dyn!(array, PrimitiveArray<f64>),
        PhysicalDataType::Binary => clone_dyn!(array, BinaryArray<i32>),
        PhysicalDataType::LargeBinary => clone_dyn!(array, BinaryArray<i64>),
        PhysicalDataType::FixedSizeBinary(_) => clone_dyn!(array, FixedSizeBinaryArray),
        PhysicalDataType::Utf8 => clone_dyn!(array, Utf8Array::<i32>),
        PhysicalDataType::LargeUtf8 => clone_dyn!(array, Utf8Array::<i64>),
        PhysicalDataType::List(_) => clone_dyn!(array, ListArray::<i32>),
        PhysicalDataType::LargeList(_) => clone_dyn!(array, ListArray::<i64>),
        PhysicalDataType::FixedSizeList(_, _) => clone_dyn!(array, FixedSizeListArray),
        PhysicalDataType::Struct(_) => clone_dyn!(array, StructArray),
        PhysicalDataType::Union(_, _, _) => clone_dyn!(array, UnionArray),
        PhysicalDataType::Dictionary(key_type, _) => {
            with_match_dictionary_key_type!(key_type.as_ref(), |$T| {
            clone_dyn!(array, DictionaryArray::<$T>)
            })
        }
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
pub use equal::equal;

pub use binary::{BinaryArray, MutableBinaryArray};
pub use boolean::{BooleanArray, MutableBooleanArray};
pub use dictionary::{DictionaryArray, DictionaryKey, MutableDictionaryArray};
pub use fixed_size_binary::{FixedSizeBinaryArray, MutableFixedSizeBinaryArray};
pub use fixed_size_list::{FixedSizeListArray, MutableFixedSizeListArray};
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

/// A trait describing the ability of a struct to receive new items.
pub trait TryPush<A> {
    /// Tries to push a new element.
    fn try_push(&mut self, item: A) -> Result<()>;
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

// backward compatibility
use std::sync::Arc;
pub type ArrayRef = Arc<dyn Array>;
