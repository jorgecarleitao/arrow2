use crate::{bitmap::Bitmap, buffer::Buffer, datatypes::DataType, error::Result};

use super::{display_fmt, display_helper, ffi::ToFfi, Array};

mod iterator;
mod mutable;
pub use mutable::*;

/// The Arrow's equivalent to an immutable `Vec<Option<[u8; size]>>`.
/// Cloning and slicing this struct is `O(1)`.
#[derive(Debug, Clone)]
pub struct FixedSizeBinaryArray {
    size: usize, // this is redundant with `data_type`, but useful to not have to deconstruct the data_type.
    data_type: DataType,
    values: Buffer<u8>,
    validity: Option<Bitmap>,
    offset: usize,
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
        let size = *Self::get_size(&data_type) as usize;

        assert_eq!(values.len() % size, 0);

        Self {
            size,
            data_type,
            values,
            validity,
            offset: 0,
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
            offset: self.offset + offset,
        }
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
        &self.values()[i * self.size as usize..(i + 1) * self.size as usize]
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

    /// Returns the size
    pub fn size(&self) -> usize {
        self.size
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

impl FixedSizeBinaryArray {
    pub(crate) fn get_size(data_type: &DataType) -> &i32 {
        match data_type {
            DataType::FixedSizeBinary(size) => size,
            DataType::Extension(_, child, _) => Self::get_size(child),
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
        self.values.len() / self.size as usize
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
    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice_unchecked(offset, length))
    }
    fn with_validity(&self, validity: Option<Bitmap>) -> Box<dyn Array> {
        Box::new(self.with_validity(validity))
    }
}

impl std::fmt::Display for FixedSizeBinaryArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let a = |x: &[u8]| display_helper(x.iter().map(|x| Some(format!("{:b}", x)))).join(" ");
        let iter = self.iter().map(|x| x.map(a));
        display_fmt(iter, "FixedSizeBinaryArray", f, false)
    }
}

unsafe impl ToFfi for FixedSizeBinaryArray {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        vec![
            self.validity.as_ref().map(|x| x.as_ptr()),
            std::ptr::NonNull::new(self.values.as_ptr() as *mut u8),
        ]
    }

    fn offset(&self) -> usize {
        self.offset
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
