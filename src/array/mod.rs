//! Contains the [`Array`] and [`MutableArray`] trait objects declaring arrays,
//! as well as concrete arrays (such as [`Utf8Array`] and [`MutableUtf8Array`]).
//!
//! Fixed-length containers with optional values
//! that are layed in memory according to the Arrow specification.
//! Each array type has its own `struct`. The following are the main array types:
//! * [`PrimitiveArray`] and [`MutablePrimitiveArray`], an array of values with a fixed length such as integers, floats, etc.
//! * [`BooleanArray`] and [`MutableBooleanArray`], an array of boolean values (stored as a bitmap)
//! * [`Utf8Array`] and [`MutableUtf8Array`], an array of variable length utf8 values
//! * [`BinaryArray`] and [`MutableBinaryArray`], an array of opaque variable length values
//! * [`ListArray`] and [`MutableListArray`], an array of arrays (e.g. `[[1, 2], None, [], [None]]`)
//! * [`StructArray`] and [`MutableStructArray`], an array of arrays identified by a string (e.g. `{"a": [1, 2], "b": [true, false]}`)
//! All immutable arrays implement the trait object [`Array`] and that can be downcasted
//! to a concrete struct based on [`PhysicalType`](crate::datatypes::PhysicalType) available from [`Array::data_type`].
//! All immutable arrays are backed by [`Buffer`](crate::buffer::Buffer) and thus cloning and slicing them is `O(1)`.
//!
//! Most arrays contain a [`MutableArray`] counterpart that is neither clonable nor slicable, but
//! can be operated in-place.
use std::any::Any;
use std::sync::Arc;

use crate::error::Result;
use crate::{
    bitmap::{Bitmap, MutableBitmap},
    datatypes::DataType,
};

pub(self) mod physical_binary;

/// A trait representing an immutable Arrow array. Arrow arrays are trait objects
/// that are infallibly downcasted to concrete types according to the [`Array::data_type`].
pub trait Array: Send + Sync + dyn_clone::DynClone + 'static {
    /// Converts itself to a reference of [`Any`], which enables downcasting to concrete types.
    fn as_any(&self) -> &dyn Any;

    /// Converts itself to a mutable reference of [`Any`], which enables mutable downcasting to concrete types.
    fn as_any_mut(&mut self) -> &mut dyn Any;

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
    fn validity(&self) -> Option<&Bitmap>;

    /// The number of null slots on this [`Array`].
    /// # Implementation
    /// This is `O(1)` since the number of null elements is pre-computed.
    #[inline]
    fn null_count(&self) -> usize {
        if self.data_type() == &DataType::Null {
            return self.len();
        };
        self.validity()
            .as_ref()
            .map(|x| x.unset_bits())
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
    /// This function panics iff `offset + length > self.len()`.
    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array>;

    /// Slices the [`Array`], returning a new `Box<dyn Array>`.
    /// # Implementation
    /// This operation is `O(1)` over `len`, as it amounts to increase two ref counts
    /// and moving the struct to the heap.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`
    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Box<dyn Array>;

    /// Clones this [`Array`] with a new new assigned bitmap.
    /// # Panic
    /// This function panics iff `validity.len() != self.len()`.
    fn with_validity(&self, validity: Option<Bitmap>) -> Box<dyn Array>;

    /// Clone a `&dyn Array` to an owned `Box<dyn Array>`.
    fn to_boxed(&self) -> Box<dyn Array>;
}

dyn_clone::clone_trait_object!(Array);

/// A trait describing an array with a backing store that can be preallocated to
/// a given size.
pub(crate) trait Container {
    /// Create this array with a given capacity.
    fn with_capacity(capacity: usize) -> Self
    where
        Self: Sized;
}

/// A trait describing a mutable array; i.e. an array whose values can be changed.
/// Mutable arrays cannot be cloned but can be mutated in place,
/// thereby making them useful to perform numeric operations without allocations.
/// As in [`Array`], concrete arrays (such as [`MutablePrimitiveArray`]) implement how they are mutated.
pub trait MutableArray: std::fmt::Debug + Send + Sync {
    /// The [`DataType`] of the array.
    fn data_type(&self) -> &DataType;

    /// The length of the array.
    fn len(&self) -> usize;

    /// Whether the array is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The optional validity of the array.
    fn validity(&self) -> Option<&MutableBitmap>;

    /// Convert itself to an (immutable) [`Array`].
    fn as_box(&mut self) -> Box<dyn Array>;

    /// Convert itself to an (immutable) atomically reference counted [`Array`].
    // This provided implementation has an extra allocation as it first
    // boxes `self`, then converts the box into an `Arc`. Implementors may wish
    // to avoid an allocation by skipping the box completely.
    fn as_arc(&mut self) -> std::sync::Arc<dyn Array> {
        self.as_box().into()
    }

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

    /// Reserves additional slots to its capacity.
    fn reserve(&mut self, additional: usize);

    /// Shrink the array to fit its length.
    fn shrink_to_fit(&mut self);
}

impl MutableArray for Box<dyn MutableArray> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        self.as_ref().validity()
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        self.as_mut().as_box()
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        self.as_mut().as_arc()
    }

    fn data_type(&self) -> &DataType {
        self.as_ref().data_type()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.as_ref().as_any()
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self.as_mut().as_mut_any()
    }

    #[inline]
    fn push_null(&mut self) {
        self.as_mut().push_null()
    }

    fn shrink_to_fit(&mut self) {
        self.as_mut().shrink_to_fit();
    }

    fn reserve(&mut self, additional: usize) {
        self.as_mut().reserve(additional);
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

macro_rules! match_integer_type {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use crate::datatypes::IntegerType::*;
    match $key_type {
        Int8 => __with_ty__! { i8 },
        Int16 => __with_ty__! { i16 },
        Int32 => __with_ty__! { i32 },
        Int64 => __with_ty__! { i64 },
        UInt8 => __with_ty__! { u8 },
        UInt16 => __with_ty__! { u16 },
        UInt32 => __with_ty__! { u32 },
        UInt64 => __with_ty__! { u64 },
    }
})}

macro_rules! with_match_primitive_type {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use crate::datatypes::PrimitiveType::*;
    use crate::types::{days_ms, months_days_ns, f16, i256};
    match $key_type {
        Int8 => __with_ty__! { i8 },
        Int16 => __with_ty__! { i16 },
        Int32 => __with_ty__! { i32 },
        Int64 => __with_ty__! { i64 },
        Int128 => __with_ty__! { i128 },
        Int256 => __with_ty__! { i256 },
        DaysMs => __with_ty__! { days_ms },
        MonthDayNano => __with_ty__! { months_days_ns },
        UInt8 => __with_ty__! { u8 },
        UInt16 => __with_ty__! { u16 },
        UInt32 => __with_ty__! { u32 },
        UInt64 => __with_ty__! { u64 },
        Float16 => __with_ty__! { f16 },
        Float32 => __with_ty__! { f32 },
        Float64 => __with_ty__! { f64 },
    }
})}

impl std::fmt::Debug for dyn Array + '_ {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use crate::datatypes::PhysicalType::*;
        match self.data_type().to_physical_type() {
            Null => fmt_dyn!(self, NullArray, f),
            Boolean => fmt_dyn!(self, BooleanArray, f),
            Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
                fmt_dyn!(self, PrimitiveArray<$T>, f)
            }),
            Binary => fmt_dyn!(self, BinaryArray<i32>, f),
            LargeBinary => fmt_dyn!(self, BinaryArray<i64>, f),
            FixedSizeBinary => fmt_dyn!(self, FixedSizeBinaryArray, f),
            Utf8 => fmt_dyn!(self, Utf8Array::<i32>, f),
            LargeUtf8 => fmt_dyn!(self, Utf8Array::<i64>, f),
            List => fmt_dyn!(self, ListArray::<i32>, f),
            LargeList => fmt_dyn!(self, ListArray::<i64>, f),
            FixedSizeList => fmt_dyn!(self, FixedSizeListArray, f),
            Struct => fmt_dyn!(self, StructArray, f),
            Union => fmt_dyn!(self, UnionArray, f),
            Dictionary(key_type) => {
                match_integer_type!(key_type, |$T| {
                    fmt_dyn!(self, DictionaryArray::<$T>, f)
                })
            }
            Map => fmt_dyn!(self, MapArray, f),
        }
    }
}

/// Creates a new [`Array`] with a [`Array::len`] of 0.
pub fn new_empty_array(data_type: DataType) -> Box<dyn Array> {
    use crate::datatypes::PhysicalType::*;
    match data_type.to_physical_type() {
        Null => Box::new(NullArray::new_empty(data_type)),
        Boolean => Box::new(BooleanArray::new_empty(data_type)),
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            Box::new(PrimitiveArray::<$T>::new_empty(data_type))
        }),
        Binary => Box::new(BinaryArray::<i32>::new_empty(data_type)),
        LargeBinary => Box::new(BinaryArray::<i64>::new_empty(data_type)),
        FixedSizeBinary => Box::new(FixedSizeBinaryArray::new_empty(data_type)),
        Utf8 => Box::new(Utf8Array::<i32>::new_empty(data_type)),
        LargeUtf8 => Box::new(Utf8Array::<i64>::new_empty(data_type)),
        List => Box::new(ListArray::<i32>::new_empty(data_type)),
        LargeList => Box::new(ListArray::<i64>::new_empty(data_type)),
        FixedSizeList => Box::new(FixedSizeListArray::new_empty(data_type)),
        Struct => Box::new(StructArray::new_empty(data_type)),
        Union => Box::new(UnionArray::new_empty(data_type)),
        Map => Box::new(MapArray::new_empty(data_type)),
        Dictionary(key_type) => {
            match_integer_type!(key_type, |$T| {
                Box::new(DictionaryArray::<$T>::new_empty(data_type))
            })
        }
    }
}

/// Creates a new [`Array`] of [`DataType`] `data_type` and `length`.
/// The array is guaranteed to have [`Array::null_count`] equal to [`Array::len`]
/// for all types except Union, which does not have a validity.
pub fn new_null_array(data_type: DataType, length: usize) -> Box<dyn Array> {
    use crate::datatypes::PhysicalType::*;
    match data_type.to_physical_type() {
        Null => Box::new(NullArray::new_null(data_type, length)),
        Boolean => Box::new(BooleanArray::new_null(data_type, length)),
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            Box::new(PrimitiveArray::<$T>::new_null(data_type, length))
        }),
        Binary => Box::new(BinaryArray::<i32>::new_null(data_type, length)),
        LargeBinary => Box::new(BinaryArray::<i64>::new_null(data_type, length)),
        FixedSizeBinary => Box::new(FixedSizeBinaryArray::new_null(data_type, length)),
        Utf8 => Box::new(Utf8Array::<i32>::new_null(data_type, length)),
        LargeUtf8 => Box::new(Utf8Array::<i64>::new_null(data_type, length)),
        List => Box::new(ListArray::<i32>::new_null(data_type, length)),
        LargeList => Box::new(ListArray::<i64>::new_null(data_type, length)),
        FixedSizeList => Box::new(FixedSizeListArray::new_null(data_type, length)),
        Struct => Box::new(StructArray::new_null(data_type, length)),
        Union => Box::new(UnionArray::new_null(data_type, length)),
        Map => Box::new(MapArray::new_null(data_type, length)),
        Dictionary(key_type) => {
            match_integer_type!(key_type, |$T| {
                Box::new(DictionaryArray::<$T>::new_null(data_type, length))
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
    use crate::datatypes::PhysicalType::*;
    match array.data_type().to_physical_type() {
        Null => clone_dyn!(array, NullArray),
        Boolean => clone_dyn!(array, BooleanArray),
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            clone_dyn!(array, PrimitiveArray<$T>)
        }),
        Binary => clone_dyn!(array, BinaryArray<i32>),
        LargeBinary => clone_dyn!(array, BinaryArray<i64>),
        FixedSizeBinary => clone_dyn!(array, FixedSizeBinaryArray),
        Utf8 => clone_dyn!(array, Utf8Array::<i32>),
        LargeUtf8 => clone_dyn!(array, Utf8Array::<i64>),
        List => clone_dyn!(array, ListArray::<i32>),
        LargeList => clone_dyn!(array, ListArray::<i64>),
        FixedSizeList => clone_dyn!(array, FixedSizeListArray),
        Struct => clone_dyn!(array, StructArray),
        Union => clone_dyn!(array, UnionArray),
        Map => clone_dyn!(array, MapArray),
        Dictionary(key_type) => {
            match_integer_type!(key_type, |$T| {
                clone_dyn!(array, DictionaryArray::<$T>)
            })
        }
    }
}

// see https://users.rust-lang.org/t/generic-for-dyn-a-or-box-dyn-a-or-arc-dyn-a/69430/3
// for details
impl<'a> AsRef<(dyn Array + 'a)> for dyn Array {
    fn as_ref(&self) -> &(dyn Array + 'a) {
        self
    }
}

mod binary;
mod boolean;
mod dictionary;
mod fixed_size_binary;
mod fixed_size_list;
mod list;
mod map;
mod null;
mod primitive;
mod specification;
mod struct_;
mod union;
mod utf8;

mod equal;
mod ffi;
mod fmt;
pub mod growable;
mod iterator;
pub mod ord;

pub(crate) use iterator::ArrayAccessor;
pub use iterator::ArrayValuesIter;

pub use equal::equal;
pub use fmt::{get_display, get_value_display};

pub use crate::types::Offset;
pub use binary::{BinaryArray, BinaryValueIter, MutableBinaryArray, MutableBinaryValuesArray};
pub use boolean::{BooleanArray, MutableBooleanArray};
pub use dictionary::{DictionaryArray, DictionaryKey, MutableDictionaryArray};
pub use fixed_size_binary::{FixedSizeBinaryArray, MutableFixedSizeBinaryArray};
pub use fixed_size_list::{FixedSizeListArray, MutableFixedSizeListArray};
pub use list::{ListArray, ListValuesIter, MutableListArray};
pub use map::MapArray;
pub use null::NullArray;
pub use primitive::*;
pub use struct_::{MutableStructArray, StructArray};
pub use union::UnionArray;
pub use utf8::{MutableUtf8Array, MutableUtf8ValuesArray, Utf8Array, Utf8ValuesIter};

pub(crate) use self::ffi::offset_buffers_children_dictionary;
pub(crate) use self::ffi::FromFfi;
pub(crate) use self::ffi::ToFfi;

/// A trait describing the ability of a struct to create itself from a iterator.
/// This is similar to [`Extend`], but accepted the creation to error.
pub trait TryExtend<A> {
    /// Fallible version of [`Extend::extend`].
    fn try_extend<I: IntoIterator<Item = A>>(&mut self, iter: I) -> Result<()>;
}

/// A trait describing the ability of a struct to receive new items.
pub trait TryPush<A> {
    /// Tries to push a new element.
    fn try_push(&mut self, item: A) -> Result<()>;
}

/// A trait describing the ability of a struct to extend from a reference of itself.
/// Specialization of [`TryExtend`].
pub trait TryExtendFromSelf {
    /// Tries to extend itself with elements from `other`, failing only on overflow.
    fn try_extend_from_self(&mut self, other: &Self) -> Result<()>;
}

/// Trait that [`BinaryArray`] and [`Utf8Array`] implement for the purposes of DRY.
/// # Safety
/// The implementer must ensure that
/// 1. `offsets.len() > 0`
/// 2. `offsets[i] >= offsets[i-1] for all i`
/// 3. `offsets[i] < values.len() for all i`
pub unsafe trait GenericBinaryArray<O: Offset>: Array {
    /// The values of the array
    fn values(&self) -> &[u8];
    /// The offsets of the array
    fn offsets(&self) -> &[O];
}
