use crate::{
    bitmap::Bitmap,
    buffer::Buffer,
    datatypes::DataType,
    error::{ArrowError, Result},
};

use super::{
    display_fmt, display_helper, specification::check_offsets, specification::Offset, Array,
    GenericBinaryArray,
};

mod ffi;
mod iterator;
pub use iterator::*;
mod from;
mod mutable;
pub use mutable::*;

/// A [`BinaryArray`] is a nullable array of bytes - the Arrow equivalent of `Vec<Option<Vec<u8>>>`.
#[derive(Debug, Clone)]
pub struct BinaryArray<O: Offset> {
    data_type: DataType,
    offsets: Buffer<O>,
    values: Buffer<u8>,
    validity: Option<Bitmap>,
    offset: usize,
}

// constructors
impl<O: Offset> BinaryArray<O> {
    /// Creates an empty [`BinaryArray`], i.e. whose `.len` is zero.
    pub fn new_empty(data_type: DataType) -> Self {
        Self::from_data(data_type, Buffer::from(&[O::zero()]), Buffer::new(), None)
    }

    /// Creates an null [`BinaryArray`], i.e. whose `.null_count() == .len()`.
    #[inline]
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        Self::from_data(
            data_type,
            Buffer::new_zeroed(length + 1),
            Buffer::new(),
            Some(Bitmap::new_zeroed(length)),
        )
    }

    /// Creates a new [`BinaryArray`] from lower-level parts
    /// # Panics
    /// * The length of the offset buffer must be larger than 1
    /// * The length of the values must be equal to the last offset value
    pub fn from_data(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Self {
        check_offsets(&offsets, values.len());

        if let Some(validity) = &validity {
            assert_eq!(offsets.len() - 1, validity.len());
        }

        if data_type.to_physical_type() != Self::default_data_type().to_physical_type() {
            panic!("BinaryArray can only be initialized with DataType::Binary or DataType::LargeBinary")
        }

        Self {
            data_type,
            offsets,
            values,
            validity,
            offset: 0,
        }
    }

    /// Returns the default [`DataType`], `DataType::Binary` or `DataType::LargeBinary`
    pub fn default_data_type() -> DataType {
        if O::is_large() {
            DataType::LargeBinary
        } else {
            DataType::Binary
        }
    }

    /// Creates a new [`BinaryArray`] by slicing this [`BinaryArray`].
    /// # Implementation
    /// This function is `O(1)`: all data will be shared between both arrays.
    /// # Panics
    /// iff `offset + length > self.len()`.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.clone().map(|x| x.slice(offset, length));
        let offsets = self.offsets.clone().slice(offset, length + 1);
        Self {
            data_type: self.data_type.clone(),
            offsets,
            values: self.values.clone(),
            validity,
            offset: self.offset + offset,
        }
    }
}

// accessors
impl<O: Offset> BinaryArray<O> {
    /// Returns the element at index `i`
    /// # Panics
    /// iff `i > self.len()`
    pub fn value(&self, i: usize) -> &[u8] {
        let offsets = self.offsets.as_slice();
        let offset = offsets[i];
        let offset_1 = offsets[i + 1];
        let length = (offset_1 - offset).to_usize();
        let offset = offset.to_usize();

        &self.values[offset..offset + length]
    }

    /// Returns the element at index `i`
    /// # Safety
    /// Assumes that the `i < self.len`.
    pub unsafe fn value_unchecked(&self, i: usize) -> &[u8] {
        let offset = *self.offsets.as_ptr().add(i);
        let offset_1 = *self.offsets.as_ptr().add(i + 1);
        let length = (offset_1 - offset).to_usize();
        let offset = offset.to_usize();

        &self.values[offset..offset + length]
    }

    /// Returns the offsets that slice `.values()` to return valid values.
    #[inline]
    pub fn offsets(&self) -> &Buffer<O> {
        &self.offsets
    }

    /// Returns all values in this array. Use `.offsets()` to slice them.
    #[inline]
    pub fn values(&self) -> &Buffer<u8> {
        &self.values
    }
}

impl<O: Offset> Array for BinaryArray<O> {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.offsets.len() - 1
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
    fn with_validity(&self, validity: Option<Bitmap>) -> Result<Box<dyn Array>> {
        if matches!(&validity, Some(bitmap) if bitmap.len() < self.len()) {
            return Err(ArrowError::InvalidArgumentError(
                "validity should be as least as large as the array".into(),
            ));
        }
        let mut arr = self.clone();
        arr.validity = validity;
        Ok(Box::new(arr))
    }
}

impl<O: Offset> std::fmt::Display for BinaryArray<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let a = |x: &[u8]| display_helper(x.iter().map(|x| Some(format!("{:b}", x)))).join(" ");
        let iter = self.iter().map(|x| x.map(a));
        let head = if O::is_large() {
            "LargeBinaryArray"
        } else {
            "BinaryArray"
        };
        display_fmt(iter, head, f, false)
    }
}

unsafe impl<O: Offset> GenericBinaryArray<O> for BinaryArray<O> {
    #[inline]
    fn values(&self) -> &[u8] {
        self.values()
    }

    #[inline]
    fn offsets(&self) -> &[O] {
        self.offsets()
    }
}
