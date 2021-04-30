use std::any::Any;
use std::fmt::Display;

use crate::error::Result;
use crate::types::days_ms;
use crate::{
    bitmap::Bitmap,
    datatypes::{DataType, IntervalUnit},
};

pub trait Array: std::fmt::Debug + Send + Sync + ToFfi {
    fn as_any(&self) -> &dyn Any;

    /// The length of the array
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The [`DataType`] of the array
    fn data_type(&self) -> &DataType;

    /// The validity of the Array
    fn validity(&self) -> &Option<Bitmap>;

    /// The number of nulls on this Array. This is useful to branch implementations
    /// to cases where the null count is zero.
    /// # Implementation
    /// This is `O(1)`.
    #[inline]
    fn null_count(&self) -> usize {
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

    /// Slices the `Array`, returning a new `Array`.
    /// # Implementation
    /// This operation is `O(1)` over `len`, as it amounts to increase two ref counts
    /// and moving the struct under a `Box`.
    /// # Panic
    /// This function panics iff `offset + length >= self.len()`.
    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array>;
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
            DataType::Union(_) => unimplemented!(),
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

/// Creates a new empty dynamic array
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
        DataType::Union(_) => unimplemented!(),
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

/// Creates a new null dynamic array of `length`.
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
        DataType::Union(_) => unimplemented!(),
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

/// Clones a dynamic `Array`.
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
        DataType::Union(_) => unimplemented!(),
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
mod fixed_size_binary;
mod fixed_size_list;
mod list;
mod null;
mod primitive;
mod specification;
mod struct_;
mod utf8;

mod equal;
mod ffi;
pub mod growable;
pub mod ord;

pub use binary::{BinaryArray, BinaryPrimitive};
pub use boolean::BooleanArray;
pub use dictionary::{DictionaryArray, DictionaryKey, DictionaryPrimitive};
pub use fixed_size_binary::{FixedSizeBinaryArray, FixedSizeBinaryPrimitive};
pub use fixed_size_list::{FixedSizeListArray, FixedSizeListPrimitive};
pub use list::{ListArray, ListPrimitive};
pub use null::NullArray;
pub use primitive::{Primitive, PrimitiveArray};
pub use specification::Offset;
pub use struct_::StructArray;
pub use utf8::{Utf8Array, Utf8Primitive};

pub use self::ffi::FromFfi;
use self::ffi::ToFfi;

pub type Int8Array = PrimitiveArray<i8>;
pub type Int16Array = PrimitiveArray<i16>;
pub type Int32Array = PrimitiveArray<i32>;
pub type Int64Array = PrimitiveArray<i64>;
pub type Int128Array = PrimitiveArray<i128>;
pub type Float32Array = PrimitiveArray<f32>;
pub type Float64Array = PrimitiveArray<f64>;
pub type UInt8Array = PrimitiveArray<u8>;
pub type UInt16Array = PrimitiveArray<u16>;
pub type UInt32Array = PrimitiveArray<u32>;
pub type UInt64Array = PrimitiveArray<u64>;

/// A trait describing the ability of a struct to convert itself to a Boxed [`Array`].
pub trait IntoArray {
    fn into_arc(self, data_type: &DataType) -> std::sync::Arc<dyn Array>;
}

/// A trait describing the ability of a struct to create itself from a falible iterator
/// This is used in the context of creating arrays from non-sized iterators
pub trait TryFromIterator<A>: Sized {
    fn try_from_iter<T: IntoIterator<Item = Result<A>>>(iter: T) -> Result<Self>;
}

/// A trait describing the ability of a struct to
pub trait Builder<T>: TryFromIterator<Option<T>> + IntoArray {
    /// Create the builder
    fn with_capacity(capacity: usize) -> Self;

    /// Push a new item to the builder.
    /// This operation may panic if the container cannot hold more items.
    /// For example, if all possible keys are exausted when building a dictionary.
    fn push(&mut self, item: Option<&T>);

    /// Falible version of `push`, on which the operation errors instead of panicking.
    #[inline]
    fn try_push(&mut self, item: Option<&T>) -> Result<()> {
        self.push(item);
        Ok(())
    }
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

/// Trait that binary and string arrays implement for the purposes of DRY.
/// # Safety
/// The implementer must ensure that
/// 1. `offsets.len() > 0`
/// 2. `offsets[i] >= offsets[i-1] for all i`
/// 3. `offsets[i] < values.len() for all i`
pub unsafe trait GenericBinaryArray<O: Offset>: Array {
    fn values(&self) -> &[u8];
    fn offsets(&self) -> &[O];
}
