use crate::{bitmap::Bitmap, buffer::Buffer, datatypes::DataType};

use super::{
    display_fmt,
    specification::{check_offsets_and_utf8, check_offsets_minimal},
    Array, GenericBinaryArray, Offset,
};

mod ffi;
mod from;
mod iterator;
mod mutable;
pub use iterator::*;
pub use mutable::*;

/// A [`Utf8Array`] is arrow's equivalent of an immutable `Vec<Option<String>>`.
/// Cloning and slicing this struct is `O(1)`.
/// # Example
/// ```
/// use arrow2::array::Utf8Array;
/// # fn main() {
/// let array = Utf8Array::<i32>::from([Some("hi"), None, Some("there")]);
/// assert_eq!(array.value(0), "hi");
/// assert_eq!(array.values().as_slice(), b"hithere".as_ref());
/// assert_eq!(array.offsets().as_slice(), &[0, 2, 2, 2 + 5]);
/// # }
/// ```
/// # Safety
/// The following invariants hold:
/// * Two consecutives `offsets` casted (`as`) to `usize` are valid slices of `values`.
/// * A slice of `values` taken from two consecutives `offsets` is valid `utf8`.
/// * `len` is equal to `validity.len()`, when defined.
#[derive(Debug, Clone)]
pub struct Utf8Array<O: Offset> {
    data_type: DataType,
    offsets: Buffer<O>,
    values: Buffer<u8>,
    validity: Option<Bitmap>,
    offset: usize,
}

impl<O: Offset> Utf8Array<O> {
    /// Returns a new empty [`Utf8Array`].
    #[inline]
    pub fn new_empty(data_type: DataType) -> Self {
        unsafe {
            Self::from_data_unchecked(data_type, Buffer::from(&[O::zero()]), Buffer::new(), None)
        }
    }

    /// Returns a new [`Utf8Array`] whose all slots are null / `None`.
    #[inline]
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        Self::from_data(
            data_type,
            Buffer::new_zeroed(length + 1),
            Buffer::new(),
            Some(Bitmap::new_zeroed(length)),
        )
    }

    /// The canonical method to create a [`Utf8Array`] out of low-end APIs.
    /// # Panics
    /// This function panics iff:
    /// * The `data_type`'s physical type is not consistent with the offset `O`.
    /// * The `offsets` and `values` are inconsistent
    /// * The `values` between `offsets` are utf8 encoded
    /// * The validity is not `None` and its length is different from `offsets.len() - 1`.
    pub fn from_data(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Self {
        check_offsets_and_utf8(&offsets, &values);
        if let Some(ref validity) = validity {
            assert_eq!(offsets.len() - 1, validity.len());
        }

        if data_type.to_physical_type() != Self::default_data_type().to_physical_type() {
            panic!("Utf8Array can only be initialized with DataType::Utf8 or DataType::LargeUtf8")
        }

        Self {
            data_type,
            offsets,
            values,
            validity,
            offset: 0,
        }
    }

    /// Returns the default [`DataType`], `DataType::Utf8` or `DataType::LargeUtf8`
    pub fn default_data_type() -> DataType {
        if O::is_large() {
            DataType::LargeUtf8
        } else {
            DataType::Utf8
        }
    }

    /// The same as [`Utf8Array::from_data`] but does not check for offsets nor utf8 validity.
    /// # Safety
    /// * `offsets` MUST be monotonically increasing; and
    /// * every slice of `values` constructed from `offsets` MUST be valid utf8
    /// # Panics
    /// This function panics iff:
    /// * The `data_type`'s physical type is not consistent with the offset `O`.
    /// * The last element of `offsets` is different from `values.len()`.
    /// * The validity is not `None` and its length is different from `offsets.len() - 1`.
    pub unsafe fn from_data_unchecked(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Self {
        check_offsets_minimal(&offsets, values.len());
        if let Some(ref validity) = validity {
            assert_eq!(offsets.len() - 1, validity.len());
        }

        if data_type.to_physical_type() != Self::default_data_type().to_physical_type() {
            panic!("Utf8Array can only be initialized with DataType::Utf8 or DataType::LargeUtf8")
        }

        Self {
            data_type,
            offsets,
            values,
            validity,
            offset: 0,
        }
    }

    /// Returns the element at index `i` as &str
    /// # Safety
    /// This function is safe iff `i < self.len`.
    pub unsafe fn value_unchecked(&self, i: usize) -> &str {
        // soundness: the invariant of the function
        let start = self.offsets.get_unchecked(i).to_usize();
        let end = self.offsets.get_unchecked(i + 1).to_usize();

        // soundness: the invariant of the struct
        let slice = self.values.get_unchecked(start..end);

        // soundness: the invariant of the struct
        std::str::from_utf8_unchecked(slice)
    }

    /// Returns the element at index `i`
    pub fn value(&self, i: usize) -> &str {
        let start = self.offsets[i].to_usize();
        let end = self.offsets[i + 1].to_usize();

        // soundness: the invariant of the struct
        let slice = unsafe { self.values.get_unchecked(start..end) };

        // soundness: we always check for utf8 soundness on constructors.
        unsafe { std::str::from_utf8_unchecked(slice) }
    }

    /// Returns a slice of this [`Utf8Array`].
    /// # Implementation
    /// This operation is `O(1)` as it amounts to essentially increase two ref counts.
    /// # Panic
    /// This function panics iff `offset + length >= self.len()`.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }
    /// Returns a slice of this [`Utf8Array`].
    /// # Implementation
    /// This operation is `O(1)` as it amounts to essentially increase two ref counts.
    /// # Safety
    /// The caller must ensure that `offset + length < self.len()`.
    pub unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        let validity = self
            .validity
            .clone()
            .map(|x| x.slice_unchecked(offset, length));
        // + 1: `length == 0` implies that we take the first offset.
        let offsets = self.offsets.clone().slice_unchecked(offset, length + 1);
        Self {
            data_type: self.data_type.clone(),
            offsets,
            values: self.values.clone(),
            validity,
            offset: self.offset + offset,
        }
    }

    /// Sets the validity bitmap on this [`Utf8Array`].
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

    /// Returns the offsets of this [`Utf8Array`].
    #[inline]
    pub fn offsets(&self) -> &Buffer<O> {
        &self.offsets
    }

    /// Returns the values of this [`Utf8Array`].
    #[inline]
    pub fn values(&self) -> &Buffer<u8> {
        &self.values
    }
}

impl<O: Offset> Array for Utf8Array<O> {
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
    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice_unchecked(offset, length))
    }
    fn with_validity(&self, validity: Option<Bitmap>) -> Box<dyn Array> {
        Box::new(self.with_validity(validity))
    }
}

impl<O: Offset> std::fmt::Display for Utf8Array<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        display_fmt(self.iter(), &format!("{}", self.data_type()), f, false)
    }
}

unsafe impl<O: Offset> GenericBinaryArray<O> for Utf8Array<O> {
    #[inline]
    fn values(&self) -> &[u8] {
        self.values()
    }

    #[inline]
    fn offsets(&self) -> &[O] {
        self.offsets()
    }
}
