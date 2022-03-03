use crate::{
    bitmap::Bitmap,
    buffer::Buffer,
    datatypes::DataType,
    error::{ArrowError, Result},
};
use either::Either;

use super::{
    specification::{try_check_offsets_and_utf8, try_check_offsets_bounds},
    Array, GenericBinaryArray, Offset,
};

mod ffi;
pub(super) mod fmt;
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

// constructors
impl<O: Offset> Utf8Array<O> {
    /// Returns a new [`Utf8Array`].
    ///
    /// # Errors
    /// This function returns an error iff:
    /// * the offsets are not monotonically increasing
    /// * The last offset is not equal to the values' length.
    /// * the validity's length is not equal to `offsets.len() - 1`.
    /// * The `data_type`'s [`crate::datatypes::PhysicalType`] is not equal to either `Utf8` or `LargeUtf8`.
    /// * The `values` between two consecutive `offsets` are not valid utf8
    /// # Implementation
    /// This function is `O(N)` - checking monotinicity and utf8 is `O(N)`
    pub fn try_new(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Result<Self> {
        try_check_offsets_and_utf8(&offsets, &values)?;
        if validity
            .as_ref()
            .map_or(false, |validity| validity.len() != offsets.len() - 1)
        {
            return Err(ArrowError::oos(
                "validity mask length must match the number of values",
            ));
        }

        if data_type.to_physical_type() != Self::default_data_type().to_physical_type() {
            return Err(ArrowError::oos(
                "Utf8Array can only be initialized with DataType::Utf8 or DataType::LargeUtf8",
            ));
        }

        Ok(Self {
            data_type,
            offsets,
            values,
            validity,
        })
    }

    /// Creates a new [`Utf8Array`].
    /// # Panics
    /// This function panics iff:
    /// * the offsets are not monotonically increasing
    /// * The last offset is not equal to the values' length.
    /// * the validity's length is not equal to `offsets.len() - 1`.
    /// * The `data_type`'s [`crate::datatypes::PhysicalType`] is not equal to either `Utf8` or `LargeUtf8`.
    /// * The `values` between two consecutive `offsets` are not valid utf8
    /// # Implementation
    /// This function is `O(N)` - checking monotinicity and utf8 is `O(N)`
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

    /// Returns a new empty [`Utf8Array`].
    #[inline]
    pub fn new_empty(data_type: DataType) -> Self {
        unsafe {
            Self::from_data_unchecked(
                data_type,
                Buffer::from(vec![O::zero()]),
                Buffer::new(),
                None,
            )
        }
    }

    /// Returns a new [`Utf8Array`] whose all slots are null / `None`.
    #[inline]
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        Self::new(
            data_type,
            Buffer::new_zeroed(length + 1),
            Buffer::new(),
            Some(Bitmap::new_zeroed(length)),
        )
    }

    /// Returns the default [`DataType`], `DataType::Utf8` or `DataType::LargeUtf8`
    pub fn default_data_type() -> DataType {
        if O::is_large() {
            DataType::LargeUtf8
        } else {
            DataType::Utf8
        }
    }
}

// unsafe constructors
impl<O: Offset> Utf8Array<O> {
    /// Creates a new [`Utf8Array`] without checking for offsets monotinicity nor utf8-validity
    ///
    /// # Errors
    /// This function returns an error iff:
    /// * The last offset is not equal to the values' length.
    /// * the validity's length is not equal to `offsets.len() - 1`.
    /// * The `data_type`'s [`crate::datatypes::PhysicalType`] is not equal to either `Utf8` or `LargeUtf8`.
    /// # Safety
    /// This function is unsound iff:
    /// * the offsets are not monotonically increasing
    /// * The `values` between two consecutive `offsets` are not valid utf8
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
            return Err(ArrowError::oos(
                "validity mask length must match the number of values",
            ));
        }

        if data_type.to_physical_type() != Self::default_data_type().to_physical_type() {
            return Err(ArrowError::oos(
                "BinaryArray can only be initialized with DataType::Utf8 or DataType::LargeUtf8",
            ));
        }

        Ok(Self {
            data_type,
            offsets,
            values,
            validity,
        })
    }

    /// Creates a new [`Utf8Array`] without checking for offsets monotinicity.
    ///
    /// # Errors
    /// This function returns an error iff:
    /// * The last offset is not equal to the values' length.
    /// * the validity's length is not equal to `offsets.len() - 1`.
    /// * The `data_type`'s [`crate::datatypes::PhysicalType`] is not equal to either `Utf8` or `LargeUtf8`.
    /// # Safety
    /// This function is unsound iff:
    /// * the offsets are not monotonically increasing
    /// * The `values` between two consecutive `offsets` are not valid utf8
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
    /// * The `values` between two consecutive `offsets` are not valid utf8
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
impl<O: Offset> Utf8Array<O> {
    /// Returns a slice of this [`Utf8Array`].
    /// # Implementation
    /// This operation is `O(1)` as it amounts to essentially increase two ref counts.
    /// # Panic
    /// This function panics iff `offset + length >= self.len()`.
    #[must_use]
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
    #[must_use]
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

    /// Try to convert this `Utf8Array` to a `MutableUtf8Array`
    pub fn into_mut(self) -> Either<Self, MutableUtf8Array<O>> {
        use Either::*;
        if let Some(bitmap) = self.validity {
            match bitmap.into_mut() {
                // Safety: invariants are preserved
                Left(bitmap) => Left(unsafe {
                    Utf8Array::new_unchecked(
                        self.data_type,
                        self.offsets,
                        self.values,
                        Some(bitmap),
                    )
                }),
                Right(mutable_bitmap) => match (self.values.into_mut(), self.offsets.into_mut()) {
                    (Left(immutable_values), Left(immutable_offsets)) => {
                        // Safety: invariants are preserved
                        Left(unsafe {
                            Utf8Array::new_unchecked(
                                self.data_type,
                                immutable_offsets,
                                immutable_values,
                                Some(mutable_bitmap.into()),
                            )
                        })
                    }
                    (Left(immutable_values), Right(mutable_offsets)) => {
                        // Safety: invariants are preserved
                        Left(unsafe {
                            Utf8Array::new_unchecked(
                                self.data_type,
                                mutable_offsets.into(),
                                immutable_values,
                                Some(mutable_bitmap.into()),
                            )
                        })
                    }
                    (Right(mutable_values), Left(immutable_offsets)) => {
                        // Safety: invariants are preserved
                        Left(unsafe {
                            Utf8Array::new_unchecked(
                                self.data_type,
                                immutable_offsets,
                                mutable_values.into(),
                                Some(mutable_bitmap.into()),
                            )
                        })
                    }
                    (Right(mutable_values), Right(mutable_offsets)) => {
                        Right(MutableUtf8Array::from_data(
                            self.data_type,
                            mutable_offsets,
                            mutable_values,
                            Some(mutable_bitmap),
                        ))
                    }
                },
            }
        } else {
            match (self.values.into_mut(), self.offsets.into_mut()) {
                (Left(immutable_values), Left(immutable_offsets)) => Left(Utf8Array::from_data(
                    self.data_type,
                    immutable_offsets,
                    immutable_values,
                    None,
                )),
                (Left(immutable_values), Right(mutable_offsets)) => Left(Utf8Array::from_data(
                    self.data_type,
                    mutable_offsets.into(),
                    immutable_values,
                    None,
                )),
                (Right(mutable_values), Left(immutable_offsets)) => Left(Utf8Array::from_data(
                    self.data_type,
                    immutable_offsets,
                    mutable_values.into(),
                    None,
                )),
                (Right(mutable_values), Right(mutable_offsets)) => {
                    Right(MutableUtf8Array::from_data(
                        self.data_type,
                        mutable_offsets,
                        mutable_values,
                        None,
                    ))
                }
            }
        }
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
