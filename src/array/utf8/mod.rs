use crate::{
    bitmap::{
        utils::{zip_validity, ZipValidity},
        Bitmap,
    },
    buffer::Buffer,
    datatypes::DataType,
    error::{Error, Result},
    trusted_len::TrustedLen,
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

/// A [`Utf8Array`] is arrow's semantic equivalent of an immutable `Vec<Option<String>>`.
/// Cloning and slicing this struct is `O(1)`.
/// # Example
/// ```
/// use arrow2::bitmap::Bitmap;
/// use arrow2::buffer::Buffer;
/// use arrow2::array::Utf8Array;
/// # fn main() {
/// let array = Utf8Array::<i32>::from([Some("hi"), None, Some("there")]);
/// assert_eq!(array.value(0), "hi");
/// assert_eq!(array.iter().collect::<Vec<_>>(), vec![Some("hi"), None, Some("there")]);
/// assert_eq!(array.values_iter().collect::<Vec<_>>(), vec!["hi", "", "there"]);
/// // the underlying representation
/// assert_eq!(array.validity(), Some(&Bitmap::from([true, false, true])));
/// assert_eq!(array.values(), &Buffer::from(b"hithere".to_vec()));
/// assert_eq!(array.offsets(), &Buffer::from(vec![0, 2, 2, 2 + 5]));
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
    /// Returns a [`Utf8Array`] from its internal representation.
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
            return Err(Error::oos(
                "validity mask length must match the number of values",
            ));
        }

        if data_type.to_physical_type() != Self::default_data_type().to_physical_type() {
            return Err(Error::oos(
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

    /// Returns a [`Utf8Array`] from a slice of `&str`.
    ///
    /// A convenience method that uses [`Self::from_trusted_len_values_iter`].
    pub fn from_slice<T: AsRef<str>, P: AsRef<[T]>>(slice: P) -> Self {
        Self::from_trusted_len_values_iter(slice.as_ref().iter())
    }

    /// Returns a new [`Utf8Array`] from a slice of `&str`.
    ///
    /// A convenience method that uses [`Self::from_trusted_len_iter`].
    // Note: this can't be `impl From` because Rust does not allow double `AsRef` on it.
    pub fn from<T: AsRef<str>, P: AsRef<[Option<T>]>>(slice: P) -> Self {
        Self::from_trusted_len_iter(slice.as_ref().iter().map(|x| x.as_ref()))
    }

    /// Returns an iterator of `Option<&str>`
    pub fn iter(&self) -> ZipValidity<&str, Utf8ValuesIter<O>> {
        zip_validity(
            Utf8ValuesIter::new(self),
            self.validity.as_ref().map(|x| x.iter()),
        )
    }

    /// Returns an iterator of `&str`
    pub fn values_iter(&self) -> Utf8ValuesIter<O> {
        Utf8ValuesIter::new(self)
    }

    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    /// Returns the value of the element at index `i`, ignoring the array's validity.
    /// # Panic
    /// This function panics iff `i >= self.len`.
    #[inline]
    pub fn value(&self, i: usize) -> &str {
        assert!(i < self.len());
        unsafe { self.value_unchecked(i) }
    }

    /// Returns the value of the element at index `i`, ignoring the array's validity.
    /// # Safety
    /// This function is safe iff `i < self.len`.
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> &str {
        // soundness: the invariant of the function
        let start = self.offsets.get_unchecked(i).to_usize();
        let end = self.offsets.get_unchecked(i + 1).to_usize();

        // soundness: the invariant of the struct
        let slice = self.values.get_unchecked(start..end);

        // soundness: the invariant of the struct
        std::str::from_utf8_unchecked(slice)
    }

    /// Returns the [`DataType`] of this array.
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the values of this [`Utf8Array`].
    #[inline]
    pub fn values(&self) -> &Buffer<u8> {
        &self.values
    }

    /// Returns the offsets of this [`Utf8Array`].
    #[inline]
    pub fn offsets(&self) -> &Buffer<O> {
        &self.offsets
    }

    /// The optional validity.
    #[inline]
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

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

    /// Boxes self into a [`Box<dyn Array>`].
    pub fn boxed(self) -> Box<dyn Array> {
        Box::new(self)
    }

    /// Boxes self into a [`std::sync::Arc<dyn Array>`].
    pub fn arced(self) -> std::sync::Arc<dyn Array> {
        std::sync::Arc::new(self)
    }

    /// Clones this [`Utf8Array`] and assigns it a new validity
    /// # Panic
    /// This function panics iff `validity.len() != self.len()`.
    pub fn with_validity(&self, validity: Option<Bitmap>) -> Self {
        if matches!(&validity, Some(bitmap) if bitmap.len() != self.len()) {
            panic!("validity's len must be equal to the array")
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
                (Left(immutable_values), Left(immutable_offsets)) => Left(unsafe {
                    Utf8Array::new_unchecked(
                        self.data_type,
                        immutable_offsets,
                        immutable_values,
                        None,
                    )
                }),
                (Left(immutable_values), Right(mutable_offsets)) => Left(unsafe {
                    Utf8Array::new_unchecked(
                        self.data_type,
                        mutable_offsets.into(),
                        immutable_values,
                        None,
                    )
                }),
                (Right(mutable_values), Left(immutable_offsets)) => Left(unsafe {
                    Utf8Array::from_data(
                        self.data_type,
                        immutable_offsets,
                        mutable_values.into(),
                        None,
                    )
                }),
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

    /// Returns a new empty [`Utf8Array`].
    ///
    /// The array is guaranteed to have no elements nor validity.
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
            vec![O::default(); 1 + length].into(),
            Buffer::new(),
            Some(Bitmap::new_zeroed(length)),
        )
    }

    /// Returns a default [`DataType`] of this array, which depends on the generic parameter `O`: `DataType::Utf8` or `DataType::LargeUtf8`
    pub fn default_data_type() -> DataType {
        if O::IS_LARGE {
            DataType::LargeUtf8
        } else {
            DataType::Utf8
        }
    }

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
            return Err(Error::oos(
                "validity mask length must match the number of values",
            ));
        }

        if data_type.to_physical_type() != Self::default_data_type().to_physical_type() {
            return Err(Error::oos(
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

    /// Returns a (non-null) [`Utf8Array`] created from a [`TrustedLen`] of `&str`.
    /// # Implementation
    /// This function is `O(N)`
    #[inline]
    pub fn from_trusted_len_values_iter<T: AsRef<str>, I: TrustedLen<Item = T>>(
        iterator: I,
    ) -> Self {
        MutableUtf8Array::<O>::from_trusted_len_values_iter(iterator).into()
    }

    /// Creates a new [`Utf8Array`] from a [`Iterator`] of `&str`.
    pub fn from_iter_values<T: AsRef<str>, I: Iterator<Item = T>>(iterator: I) -> Self {
        MutableUtf8Array::<O>::from_iter_values(iterator).into()
    }

    /// Creates a [`Utf8Array`] from an iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn from_trusted_len_iter_unchecked<I, P>(iterator: I) -> Self
    where
        P: AsRef<str>,
        I: Iterator<Item = Option<P>>,
    {
        MutableUtf8Array::<O>::from_trusted_len_iter_unchecked(iterator).into()
    }

    /// Creates a [`Utf8Array`] from an iterator of trusted length.
    #[inline]
    pub fn from_trusted_len_iter<I, P>(iterator: I) -> Self
    where
        P: AsRef<str>,
        I: TrustedLen<Item = Option<P>>,
    {
        // soundness: I is `TrustedLen`
        unsafe { Self::from_trusted_len_iter_unchecked(iterator) }
    }

    /// Creates a [`Utf8Array`] from an falible iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn try_from_trusted_len_iter_unchecked<E, I, P>(
        iterator: I,
    ) -> std::result::Result<Self, E>
    where
        P: AsRef<str>,
        I: IntoIterator<Item = std::result::Result<Option<P>, E>>,
    {
        MutableUtf8Array::<O>::try_from_trusted_len_iter_unchecked(iterator).map(|x| x.into())
    }

    /// Creates a [`Utf8Array`] from an fallible iterator of trusted length.
    #[inline]
    pub fn try_from_trusted_len_iter<E, I, P>(iter: I) -> std::result::Result<Self, E>
    where
        P: AsRef<str>,
        I: TrustedLen<Item = std::result::Result<Option<P>, E>>,
    {
        // soundness: I: TrustedLen
        unsafe { Self::try_from_trusted_len_iter_unchecked(iter) }
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

    /// Alias for [`Self::new_unchecked`]
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

    fn to_boxed(&self) -> Box<dyn Array> {
        Box::new(self.clone())
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
