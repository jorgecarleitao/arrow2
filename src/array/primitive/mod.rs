use crate::{
    bitmap::Bitmap,
    buffer::Buffer,
    datatypes::*,
    error::ArrowError,
    types::{days_ms, months_days_ns, NativeType},
};

use super::Array;

mod display;
mod ffi;
mod from_natural;
mod iterator;
pub use iterator::*;
mod mutable;
pub use mutable::*;

/// A [`PrimitiveArray`] is arrow's equivalent to `Vec<Option<T: NativeType>>`, i.e.
/// an array designed for highly performant operations on optionally nullable slots,
/// backed by a physical type of a physical byte-width, such as `i32` or `f64`.
/// The size of this struct is `O(1)` as all data is stored behind an [`std::sync::Arc`].
/// # Example
/// ```
/// use arrow2::array::PrimitiveArray;
/// # fn main() {
/// let array = PrimitiveArray::<i32>::from([Some(1), None, Some(2)]);
/// assert_eq!(array.value(0), 1);
/// assert_eq!(array.values().as_slice(), &[1, 0, 2]);
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PrimitiveArray<T: NativeType> {
    data_type: DataType,
    values: Buffer<T>,
    validity: Option<Bitmap>,
    offset: usize,
}

impl<T: NativeType> PrimitiveArray<T> {
    /// Returns a new empty [`PrimitiveArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        Self::from_data(data_type, Buffer::new(), None)
    }

    /// Returns a new [`PrimitiveArray`] whose all slots are null / `None`.
    #[inline]
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        Self::from_data(
            data_type,
            Buffer::new_zeroed(length),
            Some(Bitmap::new_zeroed(length)),
        )
    }

    /// The canonical method to create a [`PrimitiveArray`] out of low-end APIs.
    /// # Panics
    /// This function panics iff:
    /// * `data_type` is not supported by the physical type
    /// * The validity is not `None` and its length is different from the `values`'s length
    pub fn from_data(data_type: DataType, values: Buffer<T>, validity: Option<Bitmap>) -> Self {
        if !T::is_valid(&data_type) {
            Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<T>(),
                data_type
            )))
            .unwrap()
        }
        if let Some(ref validity) = validity {
            assert_eq!(values.len(), validity.len());
        }
        Self {
            data_type,
            values,
            validity,
            offset: 0,
        }
    }

    /// Returns a slice of this [`PrimitiveArray`].
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Panic
    /// This function panics iff `offset + length >= self.len()`.
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.clone().map(|x| x.slice(offset, length));
        Self {
            data_type: self.data_type.clone(),
            values: self.values.clone().slice(offset, length),
            validity,
            offset: self.offset + offset,
        }
    }

    /// The values [`Buffer`].
    #[inline]
    pub fn values(&self) -> &Buffer<T> {
        &self.values
    }

    /// Safe method to retrieve the value at slot `i`.
    /// Equivalent to `self.values()[i]`.
    #[inline]
    pub fn value(&self, i: usize) -> T {
        self.values()[i]
    }

    /// Returns the element at index `i` as `T`
    ///
    /// # Safety
    /// Caller must be sure that `i < self.len()`
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> T {
        *self.values().as_ptr().add(i)
    }

    /// Returns a new [`PrimitiveArray`] with a different logical type.
    /// This is `O(1)`.
    /// # Panics
    /// Panics iff the data_type is not supported for the physical type.
    #[inline]
    pub fn to(self, data_type: DataType) -> Self {
        if !T::is_valid(&data_type) {
            Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<T>(),
                data_type
            )))
            .unwrap()
        }
        Self {
            data_type,
            values: self.values,
            validity: self.validity,
            offset: self.offset,
        }
    }
}

impl<T: NativeType> Array for PrimitiveArray<T> {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn validity(&self) -> &Option<Bitmap> {
        &self.validity
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

/// A type definition [`PrimitiveArray`] for `i8`
pub type Int8Array = PrimitiveArray<i8>;
/// A type definition [`PrimitiveArray`] for `i16`
pub type Int16Array = PrimitiveArray<i16>;
/// A type definition [`PrimitiveArray`] for `i32`
pub type Int32Array = PrimitiveArray<i32>;
/// A type definition [`PrimitiveArray`] for `i64`
pub type Int64Array = PrimitiveArray<i64>;
/// A type definition [`PrimitiveArray`] for `i128`
pub type Int128Array = PrimitiveArray<i128>;
/// A type definition [`PrimitiveArray`] for [`days_ms`]
pub type DaysMsArray = PrimitiveArray<days_ms>;
/// A type definition [`PrimitiveArray`] for [`months_days_ns`]
pub type MonthsDaysNsArray = PrimitiveArray<months_days_ns>;
/// A type definition [`PrimitiveArray`] for `f32`
pub type Float32Array = PrimitiveArray<f32>;
/// A type definition [`PrimitiveArray`] for `f64`
pub type Float64Array = PrimitiveArray<f64>;
/// A type definition [`PrimitiveArray`] for `u8`
pub type UInt8Array = PrimitiveArray<u8>;
/// A type definition [`PrimitiveArray`] for `u16`
pub type UInt16Array = PrimitiveArray<u16>;
/// A type definition [`PrimitiveArray`] for `u32`
pub type UInt32Array = PrimitiveArray<u32>;
/// A type definition [`PrimitiveArray`] for `u64`
pub type UInt64Array = PrimitiveArray<u64>;

/// A type definition [`MutablePrimitiveArray`] for `i8`
pub type Int8Vec = MutablePrimitiveArray<i8>;
/// A type definition [`MutablePrimitiveArray`] for `i16`
pub type Int16Vec = MutablePrimitiveArray<i16>;
/// A type definition [`MutablePrimitiveArray`] for `i32`
pub type Int32Vec = MutablePrimitiveArray<i32>;
/// A type definition [`MutablePrimitiveArray`] for `i64`
pub type Int64Vec = MutablePrimitiveArray<i64>;
/// A type definition [`MutablePrimitiveArray`] for `i128`
pub type Int128Vec = MutablePrimitiveArray<i128>;
/// A type definition [`MutablePrimitiveArray`] for [`days_ms`]
pub type DaysMsVec = MutablePrimitiveArray<days_ms>;
/// A type definition [`MutablePrimitiveArray`] for [`months_days_ns`]
pub type MonthsDaysNsVec = MutablePrimitiveArray<months_days_ns>;
/// A type definition [`MutablePrimitiveArray`] for `f32`
pub type Float32Vec = MutablePrimitiveArray<f32>;
/// A type definition [`MutablePrimitiveArray`] for `f64`
pub type Float64Vec = MutablePrimitiveArray<f64>;
/// A type definition [`MutablePrimitiveArray`] for `u8`
pub type UInt8Vec = MutablePrimitiveArray<u8>;
/// A type definition [`MutablePrimitiveArray`] for `u16`
pub type UInt16Vec = MutablePrimitiveArray<u16>;
/// A type definition [`MutablePrimitiveArray`] for `u32`
pub type UInt32Vec = MutablePrimitiveArray<u32>;
/// A type definition [`MutablePrimitiveArray`] for `u64`
pub type UInt64Vec = MutablePrimitiveArray<u64>;
