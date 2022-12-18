use crate::{bitmap::Bitmap, buffer::Buffer, datatypes::DataType, error::Error};

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
    /// Creates a new [`FixedSizeBinaryArray`].
    ///
    /// # Errors
    /// This function returns an error iff:
    /// * The `data_type`'s physical type is not [`crate::datatypes::PhysicalType::FixedSizeBinary`]
    /// * The length of `values` is not a multiple of `size` in `data_type`
    /// * the validity's length is not equal to `values.len() / size`.
    pub fn try_new(
        data_type: DataType,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Result<Self, Error> {
        let size = Self::maybe_get_size(&data_type)?;

        if values.len() % size != 0 {
            return Err(Error::oos(format!(
                "values (of len {}) must be a multiple of size ({}) in FixedSizeBinaryArray.",
                values.len(),
                size
            )));
        }
        let len = values.len() / size;

        if validity
            .as_ref()
            .map_or(false, |validity| validity.len() != len)
        {
            return Err(Error::oos(
                "validity mask length must be equal to the number of values divided by size",
            ));
        }

        Ok(Self {
            size,
            data_type,
            values,
            validity,
        })
    }

    /// Creates a new [`FixedSizeBinaryArray`].
    /// # Panics
    /// This function panics iff:
    /// * The `data_type`'s physical type is not [`crate::datatypes::PhysicalType::FixedSizeBinary`]
    /// * The length of `values` is not a multiple of `size` in `data_type`
    /// * the validity's length is not equal to `values.len() / size`.
    pub fn new(data_type: DataType, values: Buffer<u8>, validity: Option<Bitmap>) -> Self {
        Self::try_new(data_type, values, validity).unwrap()
    }

    /// Returns a new empty [`FixedSizeBinaryArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        Self::new(data_type, Buffer::new(), None)
    }

    /// Returns a new null [`FixedSizeBinaryArray`].
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        let size = Self::maybe_get_size(&data_type).unwrap();
        Self::new(
            data_type,
            vec![0u8; length * size].into(),
            Some(Bitmap::new_zeroed(length)),
        )
    }

    /// Boxes self into a [`Box<dyn Array>`].
    pub fn boxed(self) -> Box<dyn Array> {
        Box::new(self)
    }

    /// Boxes self into a [`std::sync::Arc<dyn Array>`].
    pub fn arced(self) -> std::sync::Arc<dyn Array> {
        std::sync::Arc::new(self)
    }
}

// must use
impl FixedSizeBinaryArray {
    /// Returns a slice of this [`FixedSizeBinaryArray`].
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase 3 ref counts.
    /// # Panics
    /// panics iff `offset + length > self.len()`
    #[must_use]
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
    #[must_use]
    pub unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        let validity = self
            .validity
            .clone()
            .map(|bitmap| bitmap.slice_unchecked(offset, length))
            .and_then(|bitmap| (bitmap.unset_bits() > 0).then(|| bitmap));
        let values = self
            .values
            .clone()
            .slice_unchecked(offset * self.size, length * self.size);
        Self {
            data_type: self.data_type.clone(),
            size: self.size,
            values,
            validity,
        }
    }

    /// Returns this [`FixedSizeBinaryArray`] with a new validity.
    /// # Panic
    /// This function panics iff `validity.len() != self.len()`.
    #[must_use]
    pub fn with_validity(mut self, validity: Option<Bitmap>) -> Self {
        self.set_validity(validity);
        self
    }

    /// Sets the validity of this [`FixedSizeBinaryArray`].
    /// # Panics
    /// This function panics iff `validity.len() != self.len()`.
    pub fn set_validity(&mut self, validity: Option<Bitmap>) {
        if matches!(&validity, Some(bitmap) if bitmap.len() != self.len()) {
            panic!("validity must be equal to the array's length")
        }
        self.validity = validity;
    }
}

// accessors
impl FixedSizeBinaryArray {
    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len() / self.size
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

    /// Returns a new [`FixedSizeBinaryArray`] with a different logical type.
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
    pub(crate) fn maybe_get_size(data_type: &DataType) -> Result<usize, Error> {
        match data_type.to_logical_type() {
            DataType::FixedSizeBinary(size) => {
                if *size == 0 {
                    return Err(Error::oos("FixedSizeBinaryArray expects a positive size"));
                }
                Ok(*size)
            }
            _ => Err(Error::oos(
                "FixedSizeBinaryArray expects DataType::FixedSizeBinary",
            )),
        }
    }

    pub(crate) fn get_size(data_type: &DataType) -> usize {
        Self::maybe_get_size(data_type).unwrap()
    }
}

impl Array for FixedSizeBinaryArray {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
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
        Box::new(self.clone().with_validity(validity))
    }

    fn to_boxed(&self) -> Box<dyn Array> {
        Box::new(self.clone())
    }
}

impl FixedSizeBinaryArray {
    /// Creates a [`FixedSizeBinaryArray`] from an fallible iterator of optional `[u8]`.
    pub fn try_from_iter<P: AsRef<[u8]>, I: IntoIterator<Item = Option<P>>>(
        iter: I,
        size: usize,
    ) -> Result<Self, Error> {
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

    /// Creates a [`FixedSizeBinaryArray`] from a slice of arrays of bytes
    pub fn from_slice<const N: usize, P: AsRef<[[u8; N]]>>(a: P) -> Self {
        let values = a.as_ref().iter().flatten().copied().collect::<Vec<_>>();
        Self::new(DataType::FixedSizeBinary(N), values.into(), None)
    }

    /// Creates a new [`FixedSizeBinaryArray`] from a slice of optional `[u8]`.
    // Note: this can't be `impl From` because Rust does not allow double `AsRef` on it.
    pub fn from<const N: usize, P: AsRef<[Option<[u8; N]>]>>(slice: P) -> Self {
        MutableFixedSizeBinaryArray::from(slice).into()
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
        self.size
    }
}
