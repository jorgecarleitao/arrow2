use crate::{bitmap::Bitmap, buffer::Buffer, datatypes::DataType};

use super::{
    display_fmt, display_helper, specification::check_offsets,
    specification::check_offsets_minimal, specification::Offset, Array, GenericBinaryArray,
};

mod ffi;
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
#[derive(Debug, Clone)]
pub struct BinaryArray<O: Offset> {
    data_type: DataType,
    offsets: Buffer<O>,
    values: Buffer<u8>,
    validity: Option<Bitmap>,
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
    /// * the offsets are not monotonically increasing
    /// * The last offset is not equal to the values' length.
    /// * the validity's length is not equal to `offsets.len() - 1`.
    /// * The `data_type`'s physical type is not equal to `Binary` or `LargeBinary`.
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

    /// The same as [`BinaryArray::from_data`] but does not check for offsets.
    /// # Safety
    /// * `offsets` MUST be monotonically increasing
    /// # Panics
    /// This function panics iff:
    /// * The `data_type`'s physical type is not consistent with the offset `O`.
    /// * The last element of `offsets` is different from `values.len()`.
    /// * The validity is not `None` and its length is different from `offsets.len() - 1`.
    pub fn from_data_unchecked(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Self {
        check_offsets_minimal(&offsets, values.len());

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
        }
    }

    /// Creates a new [`BinaryArray`] by slicing this [`BinaryArray`].
    /// # Implementation
    /// This function is `O(1)`: all data will be shared between both arrays.
    /// # Panics
    /// iff `offset + length > self.len()`.
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
