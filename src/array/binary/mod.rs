use crate::{
    bitmap::Bitmap,
    buffer::Buffer,
    datatypes::DataType,
    error::{Error, Result},
};

use super::{
    specification::{try_check_offsets, try_check_offsets_bounds},
    Array, GenericBinaryArray, Offset,
};

mod ffi;
pub(super) mod fmt;
mod iterator;
pub use iterator::*;
mod from;
mod mutable;
pub use mutable::*;

/// A [`BinaryArray`] is a nullable array of bytes - the Arrow equivalent of `Vec<Option<Vec<u8>>>`.
/// # Safety
/// The following invariants hold:
/// * Two consecutives `offsets` casted (`as`) to `usize` are valid slices of `values`.
/// * `len` is equal to `validity.len()`, when defined.
#[derive(Clone)]
pub struct BinaryArray<O: Offset> {
    data_type: DataType,
    offsets: Buffer<O>,
    values: Buffer<u8>,
    validity: Option<Bitmap>,
}

// constructors
impl<O: Offset> BinaryArray<O> {
    /// Creates a new [`BinaryArray`].
    ///
    /// # Errors
    /// This function returns an error iff:
    /// * the offsets are not monotonically increasing
    /// * The last offset is not equal to the values' length.
    /// * the validity's length is not equal to `offsets.len() - 1`.
    /// * The `data_type`'s [`crate::datatypes::PhysicalType`] is not equal to either `Binary` or `LargeBinary`.
    /// # Implementation
    /// This function is `O(N)` - checking monotinicity is `O(N)`
    pub fn try_new(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Result<Self> {
        try_check_offsets(&offsets, values.len())?;

        if validity
            .as_ref()
            .map_or(false, |validity| validity.len() != offsets.len() - 1)
        {
            return Err(Error::oos(
                "validity mask length must match the number of values",
            ));
        }

        if data_type.to_physical_type() != Self::default_data_type().to_physical_type() {
            return Err(Error::oos(
                "BinaryArray can only be initialized with DataType::Binary or DataType::LargeBinary",
            ));
        }

        Ok(Self {
            data_type,
            offsets,
            values,
            validity,
        })
    }

    /// Creates a new [`BinaryArray`].
    /// # Panics
    /// * the offsets are not monotonically increasing
    /// * The last offset is not equal to the values' length.
    /// * the validity's length is not equal to `offsets.len() - 1`.
    /// * The `data_type`'s [`crate::datatypes::PhysicalType`] is not equal to either `Binary` or `LargeBinary`.
    /// # Implementation
    /// This function is `O(N)` - checking monotinicity is `O(N)`
    pub fn new(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Self {
        Self::try_new(data_type, offsets, values, validity).unwrap()
    }

    /// Alias for `new`
    pub fn from_data(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Self {
        Self::new(data_type, offsets, values, validity)
    }

    /// Creates an empty [`BinaryArray`], i.e. whose `.len` is zero.
    pub fn new_empty(data_type: DataType) -> Self {
        Self::new(
            data_type,
            Buffer::from(vec![O::zero()]),
            Buffer::new(),
            None,
        )
    }

    /// Creates an null [`BinaryArray`], i.e. whose `.null_count() == .len()`.
    #[inline]
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        Self::new(
            data_type,
            Buffer::new_zeroed(length + 1),
            Buffer::new(),
            Some(Bitmap::new_zeroed(length)),
        )
    }

    /// Returns the default [`DataType`], `DataType::Binary` or `DataType::LargeBinary`
    pub fn default_data_type() -> DataType {
        if O::IS_LARGE {
            DataType::LargeBinary
        } else {
            DataType::Binary
        }
    }
}

// unsafe constructors
impl<O: Offset> BinaryArray<O> {
    /// Creates a new [`BinaryArray`] without checking for offsets monotinicity.
    ///
    /// # Errors
    /// This function returns an error iff:
    /// * The last offset is not equal to the values' length.
    /// * the validity's length is not equal to `offsets.len() - 1`.
    /// * The `data_type`'s [`crate::datatypes::PhysicalType`] is not equal to either `Binary` or `LargeBinary`.
    /// # Safety
    /// This function is unsafe iff:
    /// * the offsets are not monotonically increasing
    /// # Implementation
    /// This function is `O(1)`
    pub unsafe fn try_new_unchecked(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Result<Self> {
        try_check_offsets_bounds(&offsets, values.len())?;

        if validity
            .as_ref()
            .map_or(false, |validity| validity.len() != offsets.len() - 1)
        {
            return Err(Error::oos(
                "validity mask length must match the number of values",
            ));
        }

        if data_type.to_physical_type() != Self::default_data_type().to_physical_type() {
            return Err(Error::oos(
                "BinaryArray can only be initialized with DataType::Binary or DataType::LargeBinary",
            ));
        }

        Ok(Self {
            data_type,
            offsets,
            values,
            validity,
        })
    }

    /// Creates a new [`BinaryArray`] without checking for offsets monotinicity.
    ///
    /// # Panics
    /// This function returns an error iff:
    /// * The last offset is not equal to the values' length.
    /// * the validity's length is not equal to `offsets.len() - 1`.
    /// * The `data_type`'s [`crate::datatypes::PhysicalType`] is not equal to either `Binary` or `LargeBinary`.
    /// # Safety
    /// This function is unsafe iff:
    /// * the offsets are not monotonically increasing
    /// # Implementation
    /// This function is `O(1)`
    pub unsafe fn new_unchecked(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Self {
        Self::try_new_unchecked(data_type, offsets, values, validity).unwrap()
    }

    /// Alias for [`new_unchecked`]
    /// # Safety
    /// This function is unsafe iff:
    /// * the offsets are not monotonically increasing
    pub unsafe fn from_data_unchecked(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Self {
        Self::new_unchecked(data_type, offsets, values, validity)
    }
}

// must use
impl<O: Offset> BinaryArray<O> {
    /// Creates a new [`BinaryArray`] by slicing this [`BinaryArray`].
    /// # Implementation
    /// This function is `O(1)`: all data will be shared between both arrays.
    /// # Panics
    /// iff `offset + length > self.len()`.
    #[must_use]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Creates a new [`BinaryArray`] by slicing this [`BinaryArray`].
    /// # Implementation
    /// This function is `O(1)`: all data will be shared between both arrays.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    #[must_use]
    pub unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        let validity = self
            .validity
            .clone()
            .map(|x| x.slice_unchecked(offset, length));
        let offsets = self.offsets.clone().slice_unchecked(offset, length + 1);
        Self {
            data_type: self.data_type.clone(),
            offsets,
            values: self.values.clone(),
            validity,
        }
    }

    /// Clones this [`BinaryArray`] with a different validity.
    /// # Panic
    /// Panics iff `validity.len() != self.len()`.
    #[must_use]
    pub fn with_validity(&self, validity: Option<Bitmap>) -> Self {
        if matches!(&validity, Some(bitmap) if bitmap.len() != self.len()) {
            panic!("validity's length must be equal to the array's length")
        }
        let mut arr = self.clone();
        arr.validity = validity;
        arr
    }
}

// accessors
impl<O: Offset> BinaryArray<O> {
    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    /// Returns the element at index `i`
    /// # Panics
    /// iff `i >= self.len()`
    pub fn value(&self, i: usize) -> &[u8] {
        let start = self.offsets[i].to_usize();
        let end = self.offsets[i + 1].to_usize();

        // soundness: the invariant of the struct
        unsafe { self.values.get_unchecked(start..end) }
    }

    /// Returns the element at index `i`
    /// # Safety
    /// Assumes that the `i < self.len`.
    pub unsafe fn value_unchecked(&self, i: usize) -> &[u8] {
        // soundness: the invariant of the function
        let start = self.offsets.get_unchecked(i).to_usize();
        let end = self.offsets.get_unchecked(i + 1).to_usize();

        // soundness: the invariant of the struct
        self.values.get_unchecked(start..end)
    }

    /// The optional validity.
    #[inline]
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
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
    fn to_boxed(&self) -> Box<dyn Array> {
        Box::new(self.clone())
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
