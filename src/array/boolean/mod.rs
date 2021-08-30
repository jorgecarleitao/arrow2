use crate::{
    bitmap::Bitmap,
    datatypes::{DataType, PhysicalType},
};

use super::{display_fmt, Array};

mod ffi;
mod from;
mod iterator;
mod mutable;

pub use iterator::*;
pub use mutable::*;

/// The Arrow's equivalent to an immutable `Vec<Option<bool>>`, but with `1/16` of its size.
/// Cloning and slicing this struct is `O(1)`.
#[derive(Debug, Clone)]
pub struct BooleanArray {
    data_type: DataType,
    values: Bitmap,
    validity: Option<Bitmap>,
    offset: usize,
}

impl BooleanArray {
    /// Returns a new empty [`BooleanArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        Self::from_data(data_type, Bitmap::new(), None)
    }

    /// Returns a new [`BooleanArray`] whose all slots are null / `None`.
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        let bitmap = Bitmap::new_zeroed(length);
        Self::from_data(data_type, bitmap.clone(), Some(bitmap))
    }

    /// The canonical method to create a [`BooleanArray`] out of low-end APIs.
    /// # Panics
    /// This function panics iff:
    /// * The validity is not `None` and its length is different from `values`'s length
    pub fn from_data(data_type: DataType, values: Bitmap, validity: Option<Bitmap>) -> Self {
        if let Some(ref validity) = validity {
            assert_eq!(values.len(), validity.len());
        }
        if data_type.to_physical_type() != PhysicalType::Boolean {
            panic!("BooleanArray can only be initialized with DataType::Boolean")
        }
        Self {
            data_type,
            values,
            validity,
            offset: 0,
        }
    }

    /// Returns a slice of this [`BooleanArray`].
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

    /// Returns the value at index `i`
    /// # Panic
    /// This function panics iff `i >= self.len()`.
    #[inline]
    pub fn value(&self, i: usize) -> bool {
        self.values.get_bit(i)
    }

    /// Returns the element at index `i` as bool
    /// # Safety
    /// Caller must be sure that `i < self.len()`
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> bool {
        self.values.get_bit_unchecked(i)
    }

    /// Returns the values of this [`BooleanArray`].
    #[inline]
    pub fn values(&self) -> &Bitmap {
        &self.values
    }
}

impl Array for BooleanArray {
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

    #[inline]
    fn validity(&self) -> &Option<Bitmap> {
        &self.validity
    }

    #[inline]
    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

impl std::fmt::Display for BooleanArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        display_fmt(self.iter(), "BooleanArray", f, false)
    }
}
