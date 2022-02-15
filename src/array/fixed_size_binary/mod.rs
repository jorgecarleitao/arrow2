use crate::{bitmap::Bitmap, buffer::Buffer, datatypes::DataType, error::Result};

use super::Array;

mod ffi;
pub(super) mod fmt;
mod iterator;
mod mutable;
pub use mutable::*;

/// The Arrow's equivalent to an immutable `Vec<Option<[u8; size]>>`.
/// Cloning and slicing this struct is `O(1)`.
#[derive(Clone)]
pub struct FixedSizeBinaryArray {
    size: usize, // this is redundant with `data_type`, but useful to not have to deconstruct the data_type.
    data_type: DataType,
    values: Buffer<u8>,
    validity: Option<Bitmap>,
}

impl FixedSizeBinaryArray {
    /// Returns a new empty [`FixedSizeBinaryArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        Self::from_data(data_type, Buffer::new(), None)
    }

    /// Returns a new null [`FixedSizeBinaryArray`].
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        Self::from_data(
            data_type,
            Buffer::new_zeroed(length),
            Some(Bitmap::new_zeroed(length)),
        )
    }

    /// Returns a new [`FixedSizeBinaryArray`].
    pub fn from_data(data_type: DataType, values: Buffer<u8>, validity: Option<Bitmap>) -> Self {
        let size = Self::get_size(&data_type);

        assert_eq!(values.len() % size, 0);

        if let Some(ref validity) = validity {
            assert_eq!(values.len() / size, validity.len());
        }

        Self {
            size,
            data_type,
            values,
            validity,
        }
    }

    /// Returns a slice of this [`FixedSizeBinaryArray`].
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase 3 ref counts.
    /// # Panics
    /// panics iff `offset + length > self.len()`
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Returns a slice of this [`FixedSizeBinaryArray`].
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase 3 ref counts.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    pub unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        let validity = self
            .validity
            .clone()
            .map(|x| x.slice_unchecked(offset, length));
        let values = self
            .values
            .clone()
            .slice_unchecked(offset * self.size as usize, length * self.size as usize);
        Self {
            data_type: self.data_type.clone(),
            size: self.size,
            values,
            validity,
        }
    }

    /// Sets the validity bitmap on this [`FixedSizeBinaryArray`].
    /// # Panic
    /// This function panics iff `validity.len() != self.len()`.
    pub fn with_validity(&self, validity: Option<Bitmap>) -> Self {
        if matches!(&validity, Some(bitmap) if bitmap.len() != self.len()) {
            panic!("validity should be as least as large as the array")
        }
        let mut arr = self.clone();
        arr.validity = validity;
        arr
    }
}

// accessors
impl FixedSizeBinaryArray {
    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len() / self.size as usize
    }

    /// The optional validity.
    #[inline]
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// Returns the values allocated on this [`FixedSizeBinaryArray`].
    pub fn values(&self) -> &Buffer<u8> {
        &self.values
    }

    /// Returns value at position `i`.
    /// # Panic
    /// Panics iff `i >= self.len()`.
    #[inline]
    pub fn value(&self, i: usize) -> &[u8] {
        assert!(i < self.len());
        unsafe { self.value_unchecked(i) }
    }

    /// Returns the element at index `i` as &str
    /// # Safety
    /// Assumes that the `i < self.len`.
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> &[u8] {
        // soundness: invariant of the function.
        self.values
            .get_unchecked(i * self.size..(i + 1) * self.size)
    }

    /// Returns a new [`FixedSizeBinary`] with a different logical type.
    /// This is `O(1)`.
    /// # Panics
    /// Panics iff the data_type is not supported for the physical type.
    #[inline]
    pub fn to(self, data_type: DataType) -> Self {
        match (
            data_type.to_logical_type(),
            self.data_type().to_logical_type(),
        ) {
            (DataType::FixedSizeBinary(size_a), DataType::FixedSizeBinary(size_b))
                if size_a == size_b => {}
            _ => panic!("Wrong DataType"),
        }

        Self {
            size: self.size,
            data_type,
            values: self.values,
            validity: self.validity,
        }
    }

    /// Returns the size
    pub fn size(&self) -> usize {
        self.size
    }
}

impl FixedSizeBinaryArray {
    pub(crate) fn get_size(data_type: &DataType) -> usize {
        match data_type.to_logical_type() {
            DataType::FixedSizeBinary(size) => *size,
            _ => panic!("Wrong DataType"),
        }
    }
}

impl Array for FixedSizeBinaryArray {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice_unchecked(offset, length))
    }
    fn with_validity(&self, validity: Option<Bitmap>) -> Box<dyn Array> {
        Box::new(self.with_validity(validity))
    }
}

impl FixedSizeBinaryArray {
    /// Creates a [`FixedSizeBinaryArray`] from an fallible iterator of optional `[u8]`.
    pub fn try_from_iter<P: AsRef<[u8]>, I: IntoIterator<Item = Option<P>>>(
        iter: I,
        size: usize,
    ) -> Result<Self> {
        MutableFixedSizeBinaryArray::try_from_iter(iter, size).map(|x| x.into())
    }

    /// Creates a [`FixedSizeBinaryArray`] from an iterator of optional `[u8]`.
    pub fn from_iter<P: AsRef<[u8]>, I: IntoIterator<Item = Option<P>>>(
        iter: I,
        size: usize,
    ) -> Self {
        MutableFixedSizeBinaryArray::try_from_iter(iter, size)
            .unwrap()
            .into()
    }
}

pub trait FixedSizeBinaryValues {
    fn values(&self) -> &[u8];
    fn size(&self) -> usize;
}

impl FixedSizeBinaryValues for FixedSizeBinaryArray {
    #[inline]
    fn values(&self) -> &[u8] {
        &self.values
    }

    #[inline]
    fn size(&self) -> usize {
        self.size as usize
    }
}
