use crate::{bitmap::Bitmap, buffer::Buffer, datatypes::DataType, error::ArrowError};

use super::{
    display_fmt, display_helper,
    specification::{check_offsets, check_offsets_minimal},
    Array, GenericBinaryArray, Offset,
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
        Self::try_new(
            data_type,
            Buffer::from(vec![O::zero()]),
            Buffer::new(),
            None,
        )
        .expect("All invariants to be uphold")
    }

    /// Creates an null [`BinaryArray`], i.e. whose `.null_count() == .len()`.
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        Self::try_new(
            data_type,
            Buffer::new_zeroed(length + 1),
            Buffer::new(),
            Some(Bitmap::new_zeroed(length)),
        )
        .expect("All invariants to be uphold")
    }

    /// Returns a new [`BinaryArray`]
    /// # Errors
    /// * the offsets are not monotonically increasing
    /// * The last offset is not equal to the values' length.
    /// * the validity's length is not equal to `offsets.len() - 1`.
    /// * The `data_type`'s physical type is not equal to `Binary` or `LargeBinary`.
    /// # Implementantion
    /// This function in `O(N)`, since it iterates over every offset.
    pub fn try_new(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Result<Self, ArrowError> {
        check_offsets(&offsets, values.len())?;

        if let Some(validity) = &validity {
            if validity.len() != offsets.len() - 1 {
                return Err(ArrowError::InvalidArgumentError(format!("The length of the validity ({}) must be equal to the length of offsets - 1 ({})", validity.len(), offsets.len() - 1)));
            }
        }

        if data_type.to_physical_type() != Self::default_data_type().to_physical_type() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "A BinaryArray can only be initialized with a datatype whose physical type is {:?}",
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

    /// The same as [`BinaryArray::try_new`] but does not check for offsets' boundness to values.
    /// # Error
    /// This function errors under the same conditions as `try_new` except for the offsets' monoticity.
    /// # Safety
    /// * `offsets` MUST be monotonically increasing
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
                "A BinaryArray can only be initialized with a datatype whose physical type is {:?}",
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

    /// Returns the default [`DataType`], [`DataType::Binary`] or [`DataType::LargeBinary`],
    /// of this Array.
    pub fn default_data_type() -> DataType {
        if O::is_large() {
            DataType::LargeBinary
        } else {
            DataType::Binary
        }
    }

    /// Returns a new [`BinaryArray`] that is a slice of itself. Similar to Rust's slicing.
    /// # Implementation
    /// This function is `O(1)`: all data will be shared between both arrays.
    /// # Panics
    /// iff `offset + length > self.len()`.
    pub fn slice(&self, start: usize, length: usize) -> Self {
        assert!(
            start + length <= self.len(),
            "the offset of the new Array cannot exceed the existing length"
        );
        unsafe { self.slice_unchecked(start, length) }
    }

    /// Returns a new [`BinaryArray`] that is a slice of itself. Similar to Rust's slicing.
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
