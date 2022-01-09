use crate::{bitmap::Bitmap, buffer::Buffer, datatypes::DataType, error::ArrowError};

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
#[derive(Clone)]
pub struct Utf8Array<O: Offset> {
    data_type: DataType,
    offsets: Buffer<O>,
    values: Buffer<u8>,
    validity: Option<Bitmap>,
}

impl<O: Offset> Utf8Array<O> {
    /// Returns a new empty [`Utf8Array`].
    #[inline]
    pub fn new_empty(data_type: DataType) -> Self {
        Self::try_new(
            data_type,
            Buffer::from(vec![O::zero()]),
            Buffer::new(),
            None,
        )
        .expect("All invariants to be uphold")
    }

    /// Returns a new [`Utf8Array`] whose all slots are null / `None`.
    #[inline]
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        Self::try_new(
            data_type,
            Buffer::new_zeroed(length + 1),
            Buffer::new(),
            Some(Bitmap::new_zeroed(length)),
        )
        .expect("All invariants to be uphold")
    }

    /// Returns a new [`Utf8Array`]
    /// # Errors
    /// Iff any of the following:
    /// * `offsets` is not monotonically increasing
    /// * the last offset is not equal to the values' length
    /// * the validity's length is not equal to `offsets.len() - 1`
    /// * `data_type`'s physical type is not equal to `Utf8` or `LargeUtf8`
    /// * any value between two consecutive offsets is not valid utf8
    /// # Implementantion
    /// This function in `O(N)` as it iterates over every offset and values to check for
    /// the above
    pub fn try_new(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Result<Self, ArrowError> {
        check_offsets_and_utf8(&offsets, &values)?;

        if let Some(validity) = &validity {
            if validity.len() != offsets.len() - 1 {
                return Err(ArrowError::InvalidArgumentError(format!("The length of the validity ({}) must be equal to the length of offsets - 1 ({})", validity.len(), offsets.len() - 1)));
            }
        }

        if data_type.to_physical_type() != Self::default_data_type().to_physical_type() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "A Utf8Array can only be initialized with a datatype whose physical type is {:?}",
                Self::default_data_type().to_physical_type()
            )));
        }

        Ok(Self {
            data_type,
            offsets,
            values,
            validity,
        })
    }

    /// The same as [`Utf8Array::try_new`] but does not check for offsets monoticity nor utf8 validity
    /// # Error
    /// Iff any of the following:
    /// * the last offset is not equal to the values' length
    /// * the validity's length is not equal to `offsets.len() - 1`
    /// * `data_type`'s physical type is not equal to `Utf8` or `LargeUtf8`
    /// # Safety
    /// * `offsets` MUST be monotonically increasing
    /// * every value between two consecutive offsets MUST be valid utf8
    /// # Implementantion
    /// This function in `O(1)`
    pub unsafe fn try_new_unchecked(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Result<Self, ArrowError> {
        check_offsets_minimal(&offsets, values.len())?;

        if let Some(validity) = &validity {
            if validity.len() != offsets.len() - 1 {
                return Err(ArrowError::InvalidArgumentError(format!("The length of the validity ({}) must be equal to the length of offsets - 1 ({})", validity.len(), offsets.len() - 1)));
            }
        }

        if data_type.to_physical_type() != Self::default_data_type().to_physical_type() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "A Utf8Array can only be initialized with a datatype whose physical type is {:?}",
                Self::default_data_type().to_physical_type()
            )));
        }

        Ok(Self {
            data_type,
            offsets,
            values,
            validity,
        })
    }

    /// Returns the default [`DataType`], `DataType::Utf8` or `DataType::LargeUtf8`
    pub fn default_data_type() -> DataType {
        if O::is_large() {
            DataType::LargeUtf8
        } else {
            DataType::Utf8
        }
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
    /// The caller must ensure that `offset + length <= self.len()`.
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
}

// Accessors
impl<O: Offset> Utf8Array<O> {
    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
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

    /// The optional validity.
    #[inline]
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
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

impl<O: Offset> std::fmt::Debug for Utf8Array<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        display_fmt(self.iter(), &format!("{:?}", self.data_type()), f, false)
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
