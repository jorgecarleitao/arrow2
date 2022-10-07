use crate::{
    bitmap::{
        utils::{zip_validity, BitmapIter, ZipValidity},
        Bitmap, MutableBitmap,
    },
    datatypes::{DataType, PhysicalType},
    error::Error,
    trusted_len::TrustedLen,
};
use either::Either;

use super::Array;

mod ffi;
pub(super) mod fmt;
mod from;
mod iterator;
mod mutable;

pub use iterator::*;
pub use mutable::*;

/// A [`BooleanArray`] is Arrow's semantically equivalent of an immutable `Vec<Option<bool>>`.
/// It implements [`Array`].
///
/// One way to think about a [`BooleanArray`] is `(DataType, Arc<Vec<u8>>, Option<Arc<Vec<u8>>>)`
/// where:
/// * the first item is the array's logical type
/// * the second is the immutable values
/// * the third is the immutable validity (whether a value is null or not as a bitmap).
///
/// The size of this struct is `O(1)`, as all data is stored behind an [`std::sync::Arc`].
/// # Example
/// ```
/// use arrow2::array::BooleanArray;
/// use arrow2::bitmap::Bitmap;
/// use arrow2::buffer::Buffer;
///
/// let array = BooleanArray::from([Some(true), None, Some(false)]);
/// assert_eq!(array.value(0), true);
/// assert_eq!(array.iter().collect::<Vec<_>>(), vec![Some(true), None, Some(false)]);
/// assert_eq!(array.values_iter().collect::<Vec<_>>(), vec![true, false, false]);
/// // the underlying representation
/// assert_eq!(array.values(), &Bitmap::from([true, false, false]));
/// assert_eq!(array.validity(), Some(&Bitmap::from([true, false, true])));
///
/// ```
#[derive(Clone)]
pub struct BooleanArray {
    data_type: DataType,
    values: Bitmap,
    validity: Option<Bitmap>,
}

impl BooleanArray {
    /// The canonical method to create a [`BooleanArray`] out of low-end APIs.
    /// # Errors
    /// This function errors iff:
    /// * The validity is not `None` and its length is different from `values`'s length
    /// * The `data_type`'s [`PhysicalType`] is not equal to [`PhysicalType::Boolean`].
    pub fn try_new(
        data_type: DataType,
        values: Bitmap,
        validity: Option<Bitmap>,
    ) -> Result<Self, Error> {
        if validity
            .as_ref()
            .map_or(false, |validity| validity.len() != values.len())
        {
            return Err(Error::oos(
                "validity mask length must match the number of values",
            ));
        }

        if data_type.to_physical_type() != PhysicalType::Boolean {
            return Err(Error::oos(
                "BooleanArray can only be initialized with a DataType whose physical type is Boolean",
            ));
        }

        Ok(Self {
            data_type,
            values,
            validity,
        })
    }

    /// Returns an iterator over the optional values of this [`BooleanArray`].
    #[inline]
    pub fn iter(&self) -> ZipValidity<bool, BitmapIter, BitmapIter> {
        zip_validity(
            self.values().iter(),
            self.validity.as_ref().map(|x| x.iter()),
        )
    }

    /// Returns an iterator over the values of this [`BooleanArray`].
    #[inline]
    pub fn values_iter(&self) -> BitmapIter {
        self.values().iter()
    }

    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// The values [`Bitmap`].
    /// Values on null slots are undetermined (they can be anything).
    #[inline]
    pub fn values(&self) -> &Bitmap {
        &self.values
    }

    /// Returns the optional validity.
    #[inline]
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// Returns the arrays' [`DataType`].
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the value at index `i`
    /// # Panic
    /// This function panics iff `i >= self.len()`.
    #[inline]
    pub fn value(&self, i: usize) -> bool {
        self.values.get_bit(i)
    }

    /// Returns the element at index `i` as bool
    /// # Safety
    /// Caller must be sure that `i < self.len()`
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> bool {
        self.values.get_bit_unchecked(i)
    }

    /// Returns a slice of this [`BooleanArray`].
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase up to two ref counts.
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    #[must_use]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Returns a slice of this [`BooleanArray`].
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    #[inline]
    #[must_use]
    pub unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        let validity = self
            .validity
            .clone()
            .map(|x| x.slice_unchecked(offset, length));
        Self {
            data_type: self.data_type.clone(),
            values: self.values.clone().slice_unchecked(offset, length),
            validity,
        }
    }

    /// Returns this [`BooleanArray`] with a new validity.
    /// # Panic
    /// This function panics iff `validity.len() != self.len()`.
    #[must_use]
    pub fn with_validity(mut self, validity: Option<Bitmap>) -> Self {
        self.set_validity(validity);
        self
    }

    /// Sets the validity of this [`BooleanArray`].
    /// # Panics
    /// This function panics iff `values.len() != self.len()`.
    pub fn set_validity(&mut self, validity: Option<Bitmap>) {
        if matches!(&validity, Some(bitmap) if bitmap.len() != self.len()) {
            panic!("validity must be equal to the array's length")
        }
        self.validity = validity;
    }

    /// Returns a clone of this [`BooleanArray`] with new values.
    /// # Panics
    /// This function panics iff `values.len() != self.len()`.
    #[must_use]
    pub fn with_values(&self, values: Bitmap) -> Self {
        let mut out = self.clone();
        out.set_values(values);
        out
    }

    /// Sets the values of this [`BooleanArray`].
    /// # Panics
    /// This function panics iff `values.len() != self.len()`.
    pub fn set_values(&mut self, values: Bitmap) {
        assert_eq!(
            values.len(),
            self.len(),
            "values length must be equal to this arrays length"
        );
        self.values = values;
    }

    /// Applies a function `f` to the values of this array, cloning the values
    /// iff they are being shared with others
    ///
    /// This is an API to use clone-on-write
    /// # Implementation
    /// This function is `O(f)` if the data is not being shared, and `O(N) + O(f)`
    /// if it is being shared (since it results in a `O(N)` memcopy).
    /// # Panics
    /// This function panics if the function modifies the length of the [`MutableBitmap`].
    pub fn apply_values_mut<F: Fn(&mut MutableBitmap)>(&mut self, f: F) {
        let values = std::mem::take(&mut self.values);
        let mut values = values.make_mut();
        f(&mut values);
        if let Some(validity) = &self.validity {
            assert_eq!(validity.len(), values.len());
        }
        self.values = values.into();
    }

    /// Try to convert this [`BooleanArray`] to a [`MutableBooleanArray`]
    pub fn into_mut(self) -> Either<Self, MutableBooleanArray> {
        use Either::*;

        if let Some(bitmap) = self.validity {
            match bitmap.into_mut() {
                Left(bitmap) => Left(BooleanArray::new(self.data_type, self.values, Some(bitmap))),
                Right(mutable_bitmap) => match self.values.into_mut() {
                    Left(immutable) => Left(BooleanArray::new(
                        self.data_type,
                        immutable,
                        Some(mutable_bitmap.into()),
                    )),
                    Right(mutable) => Right(MutableBooleanArray::from_data(
                        self.data_type,
                        mutable,
                        Some(mutable_bitmap),
                    )),
                },
            }
        } else {
            match self.values.into_mut() {
                Left(immutable) => Left(BooleanArray::new(self.data_type, immutable, None)),
                Right(mutable) => Right(MutableBooleanArray::from_data(
                    self.data_type,
                    mutable,
                    None,
                )),
            }
        }
    }

    /// Returns a new empty [`BooleanArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        Self::new(data_type, Bitmap::new(), None)
    }

    /// Returns a new [`BooleanArray`] whose all slots are null / `None`.
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        let bitmap = Bitmap::new_zeroed(length);
        Self::new(data_type, bitmap.clone(), Some(bitmap))
    }

    /// Creates a new [`BooleanArray`] from an [`TrustedLen`] of `bool`.
    #[inline]
    pub fn from_trusted_len_values_iter<I: TrustedLen<Item = bool>>(iterator: I) -> Self {
        MutableBooleanArray::from_trusted_len_values_iter(iterator).into()
    }

    /// Creates a new [`BooleanArray`] from an [`TrustedLen`] of `bool`.
    /// Use this over [`BooleanArray::from_trusted_len_iter`] when the iterator is trusted len
    /// but this crate does not mark it as such.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn from_trusted_len_values_iter_unchecked<I: Iterator<Item = bool>>(
        iterator: I,
    ) -> Self {
        MutableBooleanArray::from_trusted_len_values_iter_unchecked(iterator).into()
    }

    /// Creates a new [`BooleanArray`] from a slice of `bool`.
    #[inline]
    pub fn from_slice<P: AsRef<[bool]>>(slice: P) -> Self {
        MutableBooleanArray::from_slice(slice).into()
    }

    /// Creates a [`BooleanArray`] from an iterator of trusted length.
    /// Use this over [`BooleanArray::from_trusted_len_iter`] when the iterator is trusted len
    /// but this crate does not mark it as such.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn from_trusted_len_iter_unchecked<I, P>(iterator: I) -> Self
    where
        P: std::borrow::Borrow<bool>,
        I: Iterator<Item = Option<P>>,
    {
        MutableBooleanArray::from_trusted_len_iter_unchecked(iterator).into()
    }

    /// Creates a [`BooleanArray`] from a [`TrustedLen`].
    #[inline]
    pub fn from_trusted_len_iter<I, P>(iterator: I) -> Self
    where
        P: std::borrow::Borrow<bool>,
        I: TrustedLen<Item = Option<P>>,
    {
        MutableBooleanArray::from_trusted_len_iter(iterator).into()
    }

    /// Creates a [`BooleanArray`] from an falible iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn try_from_trusted_len_iter_unchecked<E, I, P>(iterator: I) -> Result<Self, E>
    where
        P: std::borrow::Borrow<bool>,
        I: Iterator<Item = Result<Option<P>, E>>,
    {
        Ok(MutableBooleanArray::try_from_trusted_len_iter_unchecked(iterator)?.into())
    }

    /// Creates a [`BooleanArray`] from a [`TrustedLen`].
    #[inline]
    pub fn try_from_trusted_len_iter<E, I, P>(iterator: I) -> Result<Self, E>
    where
        P: std::borrow::Borrow<bool>,
        I: TrustedLen<Item = Result<Option<P>, E>>,
    {
        Ok(MutableBooleanArray::try_from_trusted_len_iter(iterator)?.into())
    }

    /// Boxes self into a [`Box<dyn Array>`].
    pub fn boxed(self) -> Box<dyn Array> {
        Box::new(self)
    }

    /// Boxes self into a [`std::sync::Arc<dyn Array>`].
    pub fn arced(self) -> std::sync::Arc<dyn Array> {
        std::sync::Arc::new(self)
    }

    /// Returns its internal representation
    #[must_use]
    pub fn into_inner(self) -> (DataType, Bitmap, Option<Bitmap>) {
        let Self {
            data_type,
            values,
            validity,
        } = self;
        (data_type, values, validity)
    }

    /// The canonical method to create a [`BooleanArray`]
    /// # Panics
    /// This function errors iff:
    /// * The validity is not `None` and its length is different from `values`'s length
    /// * The `data_type`'s [`PhysicalType`] is not equal to [`PhysicalType::Boolean`].
    pub fn new(data_type: DataType, values: Bitmap, validity: Option<Bitmap>) -> Self {
        Self::try_new(data_type, values, validity).unwrap()
    }

    /// Alias for `new`
    pub fn from_data(data_type: DataType, values: Bitmap, validity: Option<Bitmap>) -> Self {
        Self::new(data_type, values, validity)
    }
}

impl Array for BooleanArray {
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

    #[inline]
    fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    #[inline]
    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
    #[inline]
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
