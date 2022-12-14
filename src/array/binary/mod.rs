use crate::{
    bitmap::{
        utils::{BitmapIter, ZipValidity},
        Bitmap,
    },
    buffer::Buffer,
    datatypes::DataType,
    error::Error,
    offset::{Offset, Offsets, OffsetsBuffer},
    trusted_len::TrustedLen,
};

use either::Either;

use super::{specification::try_check_offsets_bounds, Array, GenericBinaryArray};

mod ffi;
pub(super) mod fmt;
mod iterator;
pub use iterator::*;
mod from;
mod mutable_values;
pub use mutable_values::*;
mod mutable;
pub use mutable::*;

/// A [`BinaryArray`] is Arrow's semantically equivalent of an immutable `Vec<Option<Vec<u8>>>`.
/// It implements [`Array`].
///
/// The size of this struct is `O(1)`, as all data is stored behind an [`std::sync::Arc`].
/// # Example
/// ```
/// use arrow2::array::BinaryArray;
/// use arrow2::bitmap::Bitmap;
/// use arrow2::buffer::Buffer;
///
/// let array = BinaryArray::<i32>::from([Some([1, 2].as_ref()), None, Some([3].as_ref())]);
/// assert_eq!(array.value(0), &[1, 2]);
/// assert_eq!(array.iter().collect::<Vec<_>>(), vec![Some([1, 2].as_ref()), None, Some([3].as_ref())]);
/// assert_eq!(array.values_iter().collect::<Vec<_>>(), vec![[1, 2].as_ref(), &[], &[3]]);
/// // the underlying representation:
/// assert_eq!(array.values(), &Buffer::from(vec![1, 2, 3]));
/// assert_eq!(array.offsets().buffer(), &Buffer::from(vec![0, 2, 2, 3]));
/// assert_eq!(array.validity(), Some(&Bitmap::from([true, false, true])));
/// ```
///
/// # Generic parameter
/// The generic parameter [`Offset`] can only be `i32` or `i64` and tradeoffs maximum array length with
/// memory usage:
/// * the sum of lengths of all elements cannot exceed `Offset::MAX`
/// * the total size of the underlying data is `array.len() * size_of::<Offset>() + sum of lengths of all elements`
///
/// # Safety
/// The following invariants hold:
/// * Two consecutives `offsets` casted (`as`) to `usize` are valid slices of `values`.
/// * `len` is equal to `validity.len()`, when defined.
#[derive(Clone)]
pub struct BinaryArray<O: Offset> {
    data_type: DataType,
    offsets: OffsetsBuffer<O>,
    values: Buffer<u8>,
    validity: Option<Bitmap>,
}

impl<O: Offset> BinaryArray<O> {
    /// Returns a [`BinaryArray`] created from its internal representation.
    ///
    /// # Errors
    /// This function returns an error iff:
    /// * The last offset is not equal to the values' length.
    /// * the validity's length is not equal to `offsets.len()`.
    /// * The `data_type`'s [`crate::datatypes::PhysicalType`] is not equal to either `Binary` or `LargeBinary`.
    /// # Implementation
    /// This function is `O(1)`
    pub fn try_new(
        data_type: DataType,
        offsets: OffsetsBuffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Result<Self, Error> {
        try_check_offsets_bounds(&offsets, values.len())?;

        if validity
            .as_ref()
            .map_or(false, |validity| validity.len() != offsets.len())
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

    /// Creates a new [`BinaryArray`] from slices of `&[u8]`.
    pub fn from_slice<T: AsRef<[u8]>, P: AsRef<[T]>>(slice: P) -> Self {
        Self::from_trusted_len_values_iter(slice.as_ref().iter())
    }

    /// Creates a new [`BinaryArray`] from a slice of optional `&[u8]`.
    // Note: this can't be `impl From` because Rust does not allow double `AsRef` on it.
    pub fn from<T: AsRef<[u8]>, P: AsRef<[Option<T>]>>(slice: P) -> Self {
        MutableBinaryArray::<O>::from(slice).into()
    }

    /// Returns an iterator of `Option<&[u8]>` over every element of this array.
    pub fn iter(&self) -> ZipValidity<&[u8], BinaryValueIter<O>, BitmapIter> {
        ZipValidity::new_with_validity(self.values_iter(), self.validity.as_ref())
    }

    /// Returns an iterator of `&[u8]` over every element of this array, ignoring the validity
    pub fn values_iter(&self) -> BinaryValueIter<O> {
        BinaryValueIter::new(self)
    }

    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Returns the element at index `i`
    /// # Panics
    /// iff `i >= self.len()`
    #[inline]
    pub fn value(&self, i: usize) -> &[u8] {
        assert!(i < self.len());
        unsafe { self.value_unchecked(i) }
    }

    /// Returns the element at index `i`
    /// # Safety
    /// Assumes that the `i < self.len`.
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> &[u8] {
        // soundness: the invariant of the function
        let (start, end) = self.offsets.start_end_unchecked(i);

        // soundness: the invariant of the struct
        self.values.get_unchecked(start..end)
    }

    /// Returns the [`DataType`] of this array.
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the values of this [`BinaryArray`].
    #[inline]
    pub fn values(&self) -> &Buffer<u8> {
        &self.values
    }

    /// Returns the offsets of this [`BinaryArray`].
    #[inline]
    pub fn offsets(&self) -> &OffsetsBuffer<O> {
        &self.offsets
    }

    /// The optional validity.
    #[inline]
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

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
            .map(|bitmap| bitmap.slice_unchecked(offset, length))
            .and_then(|bitmap| (bitmap.unset_bits() > 0).then(|| bitmap));
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

    /// Returns this [`BinaryArray`] with a new validity.
    /// # Panic
    /// Panics iff `validity.len() != self.len()`.
    #[must_use]
    pub fn with_validity(mut self, validity: Option<Bitmap>) -> Self {
        self.set_validity(validity);
        self
    }

    /// Sets the validity of this [`BinaryArray`].
    /// # Panics
    /// This function panics iff `values.len() != self.len()`.
    pub fn set_validity(&mut self, validity: Option<Bitmap>) {
        if matches!(&validity, Some(bitmap) if bitmap.len() != self.len()) {
            panic!("validity must be equal to the array's length")
        }
        self.validity = validity;
    }

    /// Try to convert this `BinaryArray` to a `MutableBinaryArray`
    pub fn into_mut(mut self) -> Either<Self, MutableBinaryArray<O>> {
        use Either::*;
        if let Some(bitmap) = self.validity {
            match bitmap.into_mut() {
                // Safety: invariants are preserved
                Left(bitmap) => Left(BinaryArray::new(
                    self.data_type,
                    self.offsets,
                    self.values,
                    Some(bitmap),
                )),
                Right(mutable_bitmap) => match (
                    self.values.get_mut().map(std::mem::take),
                    self.offsets.get_mut(),
                ) {
                    (None, None) => Left(BinaryArray::new(
                        self.data_type,
                        self.offsets,
                        self.values,
                        Some(mutable_bitmap.into()),
                    )),
                    (None, Some(offsets)) => Left(BinaryArray::new(
                        self.data_type,
                        offsets.into(),
                        self.values,
                        Some(mutable_bitmap.into()),
                    )),
                    (Some(mutable_values), None) => Left(BinaryArray::new(
                        self.data_type,
                        self.offsets,
                        mutable_values.into(),
                        Some(mutable_bitmap.into()),
                    )),
                    (Some(values), Some(offsets)) => Right(
                        MutableBinaryArray::try_new(
                            self.data_type,
                            offsets,
                            values,
                            Some(mutable_bitmap),
                        )
                        .unwrap(),
                    ),
                },
            }
        } else {
            match (
                self.values.get_mut().map(std::mem::take),
                self.offsets.get_mut(),
            ) {
                (None, None) => Left(BinaryArray::new(
                    self.data_type,
                    self.offsets,
                    self.values,
                    None,
                )),
                (None, Some(offsets)) => Left(BinaryArray::new(
                    self.data_type,
                    offsets.into(),
                    self.values,
                    None,
                )),
                (Some(values), None) => Left(BinaryArray::new(
                    self.data_type,
                    self.offsets,
                    values.into(),
                    None,
                )),
                (Some(values), Some(offsets)) => Right(
                    MutableBinaryArray::try_new(self.data_type, offsets, values, None).unwrap(),
                ),
            }
        }
    }

    /// Creates an empty [`BinaryArray`], i.e. whose `.len` is zero.
    pub fn new_empty(data_type: DataType) -> Self {
        Self::new(data_type, OffsetsBuffer::new(), Buffer::new(), None)
    }

    /// Creates an null [`BinaryArray`], i.e. whose `.null_count() == .len()`.
    #[inline]
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        Self::new(
            data_type,
            Offsets::new_zeroed(length).into(),
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

    /// Alias for unwrapping [`Self::try_new`]
    pub fn new(
        data_type: DataType,
        offsets: OffsetsBuffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Self {
        Self::try_new(data_type, offsets, values, validity).unwrap()
    }

    /// Returns a [`BinaryArray`] from an iterator of trusted length.
    ///
    /// The [`BinaryArray`] is guaranteed to not have a validity
    #[inline]
    pub fn from_trusted_len_values_iter<T: AsRef<[u8]>, I: TrustedLen<Item = T>>(
        iterator: I,
    ) -> Self {
        MutableBinaryArray::<O>::from_trusted_len_values_iter(iterator).into()
    }

    /// Returns a new [`BinaryArray`] from a [`Iterator`] of `&[u8]`.
    ///
    /// The [`BinaryArray`] is guaranteed to not have a validity
    pub fn from_iter_values<T: AsRef<[u8]>, I: Iterator<Item = T>>(iterator: I) -> Self {
        MutableBinaryArray::<O>::from_iter_values(iterator).into()
    }

    /// Creates a [`BinaryArray`] from an iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn from_trusted_len_iter_unchecked<I, P>(iterator: I) -> Self
    where
        P: AsRef<[u8]>,
        I: Iterator<Item = Option<P>>,
    {
        MutableBinaryArray::<O>::from_trusted_len_iter_unchecked(iterator).into()
    }

    /// Creates a [`BinaryArray`] from a [`TrustedLen`]
    #[inline]
    pub fn from_trusted_len_iter<I, P>(iterator: I) -> Self
    where
        P: AsRef<[u8]>,
        I: TrustedLen<Item = Option<P>>,
    {
        // soundness: I is `TrustedLen`
        unsafe { Self::from_trusted_len_iter_unchecked(iterator) }
    }

    /// Creates a [`BinaryArray`] from an falible iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn try_from_trusted_len_iter_unchecked<E, I, P>(iterator: I) -> Result<Self, E>
    where
        P: AsRef<[u8]>,
        I: IntoIterator<Item = Result<Option<P>, E>>,
    {
        MutableBinaryArray::<O>::try_from_trusted_len_iter_unchecked(iterator).map(|x| x.into())
    }

    /// Creates a [`BinaryArray`] from an fallible iterator of trusted length.
    #[inline]
    pub fn try_from_trusted_len_iter<E, I, P>(iter: I) -> Result<Self, E>
    where
        P: AsRef<[u8]>,
        I: TrustedLen<Item = Result<Option<P>, E>>,
    {
        // soundness: I: TrustedLen
        unsafe { Self::try_from_trusted_len_iter_unchecked(iter) }
    }
}

impl<O: Offset> Array for BinaryArray<O> {
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

unsafe impl<O: Offset> GenericBinaryArray<O> for BinaryArray<O> {
    #[inline]
    fn values(&self) -> &[u8] {
        self.values()
    }

    #[inline]
    fn offsets(&self) -> &[O] {
        self.offsets().buffer()
    }
}
