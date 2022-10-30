use std::{iter::FromIterator, sync::Arc};

use crate::{
    array::{Array, MutableArray, Offset, TryExtend, TryExtendFromSelf, TryPush},
    bitmap::{
        utils::{BitmapIter, ZipValidity},
        Bitmap, MutableBitmap,
    },
    datatypes::DataType,
    error::{Error, Result},
    trusted_len::TrustedLen,
};

use super::{BinaryArray, MutableBinaryValuesArray, MutableBinaryValuesIter};
use crate::array::physical_binary::*;

/// The Arrow's equivalent to `Vec<Option<Vec<u8>>>`.
/// Converting a [`MutableBinaryArray`] into a [`BinaryArray`] is `O(1)`.
/// # Implementation
/// This struct does not allocate a validity until one is required (i.e. push a null to it).
#[derive(Debug, Clone)]
pub struct MutableBinaryArray<O: Offset> {
    values: MutableBinaryValuesArray<O>,
    validity: Option<MutableBitmap>,
}

impl<O: Offset> From<MutableBinaryArray<O>> for BinaryArray<O> {
    fn from(other: MutableBinaryArray<O>) -> Self {
        let validity = other.validity.and_then(|x| {
            let validity: Option<Bitmap> = x.into();
            validity
        });
        let array: BinaryArray<O> = other.values.into();
        array.with_validity(validity)
    }
}

impl<O: Offset> Default for MutableBinaryArray<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Offset> MutableBinaryArray<O> {
    /// Creates a new empty [`MutableBinaryArray`].
    /// # Implementation
    /// This allocates a [`Vec`] of one element
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Returns a [`MutableBinaryArray`] created from its internal representation.
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
        offsets: Vec<O>,
        values: Vec<u8>,
        validity: Option<MutableBitmap>,
    ) -> Result<Self> {
        let values = MutableBinaryValuesArray::try_new(data_type, offsets, values)?;

        if validity
            .as_ref()
            .map_or(false, |validity| validity.len() != values.len())
        {
            return Err(Error::oos(
                "validity's length must be equal to the number of values",
            ));
        }

        Ok(Self { values, validity })
    }

    /// Create a [`MutableBinaryArray`] out of its inner attributes.
    /// # Safety
    /// The caller must ensure that every value between offsets is a valid utf8.
    /// # Panics
    /// This function panics iff:
    /// * The `offsets` and `values` are inconsistent
    /// * The validity is not `None` and its length is different from `offsets`'s length minus one.
    pub unsafe fn new_unchecked(
        data_type: DataType,
        offsets: Vec<O>,
        values: Vec<u8>,
        validity: Option<MutableBitmap>,
    ) -> Self {
        let values = MutableBinaryValuesArray::new_unchecked(data_type, offsets, values);
        if let Some(ref validity) = validity {
            assert_eq!(values.len(), validity.len());
        }
        Self { values, validity }
    }

    /// Creates a new [`MutableBinaryArray`] from a slice of optional `&[u8]`.
    // Note: this can't be `impl From` because Rust does not allow double `AsRef` on it.
    pub fn from<T: AsRef<[u8]>, P: AsRef<[Option<T>]>>(slice: P) -> Self {
        Self::from_trusted_len_iter(slice.as_ref().iter().map(|x| x.as_ref()))
    }

    fn default_data_type() -> DataType {
        BinaryArray::<O>::default_data_type()
    }

    /// Initializes a new [`MutableBinaryArray`] with a pre-allocated capacity of slots.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacities(capacity, 0)
    }

    /// Initializes a new [`MutableBinaryArray`] with a pre-allocated capacity of slots and values.
    /// # Implementation
    /// This does not allocate the validity.
    pub fn with_capacities(capacity: usize, values: usize) -> Self {
        Self {
            values: MutableBinaryValuesArray::with_capacities(capacity, values),
            validity: None,
        }
    }

    /// Reserves `additional` elements and `additional_values` on the values buffer.
    pub fn reserve(&mut self, additional: usize, additional_values: usize) {
        self.values.reserve(additional, additional_values);
        if let Some(x) = self.validity.as_mut() {
            x.reserve(additional)
        }
    }

    /// Pushes a new element to the array.
    /// # Panic
    /// This operation panics iff the length of all values (in bytes) exceeds `O` maximum value.
    pub fn push<T: AsRef<[u8]>>(&mut self, value: Option<T>) {
        self.try_push(value).unwrap()
    }

    /// Pop the last entry from [`MutableBinaryArray`].
    /// This function returns `None` iff this array is empty
    pub fn pop(&mut self) -> Option<Vec<u8>> {
        let value = self.values.pop()?;
        self.validity
            .as_mut()
            .map(|x| x.pop()?.then(|| ()))
            .unwrap_or_else(|| Some(()))
            .map(|_| value)
    }

    fn try_from_iter<P: AsRef<[u8]>, I: IntoIterator<Item = Option<P>>>(iter: I) -> Result<Self> {
        let iterator = iter.into_iter();
        let (lower, _) = iterator.size_hint();
        let mut primitive = Self::with_capacity(lower);
        for item in iterator {
            primitive.try_push(item.as_ref())?
        }
        Ok(primitive)
    }

    fn init_validity(&mut self) {
        let mut validity = MutableBitmap::with_capacity(self.values.capacity());
        validity.extend_constant(self.len(), true);
        validity.set(self.len() - 1, false);
        self.validity = Some(validity);
    }

    /// Converts itself into an [`Array`].
    pub fn into_arc(self) -> Arc<dyn Array> {
        let a: BinaryArray<O> = self.into();
        Arc::new(a)
    }

    /// Shrinks the capacity of the [`MutableBinaryArray`] to fit its current length.
    pub fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit();
        if let Some(validity) = &mut self.validity {
            validity.shrink_to_fit()
        }
    }

    /// Equivalent to `Self::try_new(...).unwrap()`
    pub fn from_data(
        data_type: DataType,
        offsets: Vec<O>,
        values: Vec<u8>,
        validity: Option<MutableBitmap>,
    ) -> Self {
        Self::try_new(data_type, offsets, values, validity).unwrap()
    }
}

impl<O: Offset> MutableBinaryArray<O> {
    /// returns its values.
    pub fn values(&self) -> &Vec<u8> {
        self.values.values()
    }

    /// returns its offsets.
    pub fn offsets(&self) -> &Vec<O> {
        self.values.offsets()
    }

    /// Returns an iterator of `Option<&[u8]>`
    pub fn iter(&self) -> ZipValidity<&[u8], MutableBinaryValuesIter<O>, BitmapIter> {
        ZipValidity::new(self.values_iter(), self.validity.as_ref().map(|x| x.iter()))
    }

    /// Returns an iterator over the values of this array
    pub fn values_iter(&self) -> MutableBinaryValuesIter<O> {
        self.values.iter()
    }
}

impl<O: Offset> MutableArray for MutableBinaryArray<O> {
    fn len(&self) -> usize {
        self.values.len()
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        self.validity.as_ref()
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        // Safety:
        // `MutableBinaryArray` has the same invariants as `BinaryArray` and thus
        // `BinaryArray` can be safely created from `MutableBinaryArray` without checks.
        let (data_type, offsets, values) = std::mem::take(&mut self.values).into_inner();
        unsafe {
            BinaryArray::new_unchecked(
                data_type,
                offsets.into(),
                values.into(),
                std::mem::take(&mut self.validity).map(|x| x.into()),
            )
        }
        .boxed()
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        // Safety:
        // `MutableBinaryArray` has the same invariants as `BinaryArray` and thus
        // `BinaryArray` can be safely created from `MutableBinaryArray` without checks.
        let (data_type, offsets, values) = std::mem::take(&mut self.values).into_inner();
        unsafe {
            BinaryArray::new_unchecked(
                data_type,
                offsets.into(),
                values.into(),
                std::mem::take(&mut self.validity).map(|x| x.into()),
            )
        }
        .arced()
    }

    fn data_type(&self) -> &DataType {
        self.values.data_type()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    #[inline]
    fn push_null(&mut self) {
        self.push::<&[u8]>(None)
    }

    fn reserve(&mut self, additional: usize) {
        self.reserve(additional, 0)
    }

    fn shrink_to_fit(&mut self) {
        self.shrink_to_fit()
    }
}

impl<O: Offset, P: AsRef<[u8]>> FromIterator<Option<P>> for MutableBinaryArray<O> {
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        Self::try_from_iter(iter).unwrap()
    }
}

impl<O: Offset> MutableBinaryArray<O> {
    /// Creates a [`MutableBinaryArray`] from an iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn from_trusted_len_iter_unchecked<I, P>(iterator: I) -> Self
    where
        P: AsRef<[u8]>,
        I: Iterator<Item = Option<P>>,
    {
        let (validity, offsets, values) = trusted_len_unzip(iterator);

        Self::from_data(Self::default_data_type(), offsets, values, validity)
    }

    /// Creates a [`MutableBinaryArray`] from an iterator of trusted length.
    #[inline]
    pub fn from_trusted_len_iter<I, P>(iterator: I) -> Self
    where
        P: AsRef<[u8]>,
        I: TrustedLen<Item = Option<P>>,
    {
        // soundness: I is `TrustedLen`
        unsafe { Self::from_trusted_len_iter_unchecked(iterator) }
    }

    /// Creates a new [`BinaryArray`] from a [`TrustedLen`] of `&[u8]`.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn from_trusted_len_values_iter_unchecked<T: AsRef<[u8]>, I: Iterator<Item = T>>(
        iterator: I,
    ) -> Self {
        let (offsets, values) = unsafe { trusted_len_values_iter(iterator) };
        Self::from_data(Self::default_data_type(), offsets, values, None)
    }

    /// Creates a new [`BinaryArray`] from a [`TrustedLen`] of `&[u8]`.
    #[inline]
    pub fn from_trusted_len_values_iter<T: AsRef<[u8]>, I: TrustedLen<Item = T>>(
        iterator: I,
    ) -> Self {
        // soundness: I is `TrustedLen`
        unsafe { Self::from_trusted_len_values_iter_unchecked(iterator) }
    }

    /// Creates a [`MutableBinaryArray`] from an falible iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn try_from_trusted_len_iter_unchecked<E, I, P>(
        iterator: I,
    ) -> std::result::Result<Self, E>
    where
        P: AsRef<[u8]>,
        I: IntoIterator<Item = std::result::Result<Option<P>, E>>,
    {
        let iterator = iterator.into_iter();

        // soundness: assumed trusted len
        let (mut validity, offsets, values) = try_trusted_len_unzip(iterator)?;

        if validity.as_mut().unwrap().unset_bits() == 0 {
            validity = None;
        }

        Ok(Self::from_data(
            Self::default_data_type(),
            offsets,
            values,
            validity,
        ))
    }

    /// Creates a [`MutableBinaryArray`] from an falible iterator of trusted length.
    #[inline]
    pub fn try_from_trusted_len_iter<E, I, P>(iterator: I) -> std::result::Result<Self, E>
    where
        P: AsRef<[u8]>,
        I: TrustedLen<Item = std::result::Result<Option<P>, E>>,
    {
        // soundness: I: TrustedLen
        unsafe { Self::try_from_trusted_len_iter_unchecked(iterator) }
    }

    /// Extends the [`MutableBinaryArray`] from an iterator of trusted length.
    /// This differs from `extend_trusted_len` which accepts iterator of optional values.
    #[inline]
    pub fn extend_trusted_len_values<I, P>(&mut self, iterator: I)
    where
        P: AsRef<[u8]>,
        I: TrustedLen<Item = P>,
    {
        // Safety: The iterator is `TrustedLen`
        unsafe { self.extend_trusted_len_values_unchecked(iterator) }
    }

    /// Extends the [`MutableBinaryArray`] from an iterator of values.
    /// This differs from `extended_trusted_len` which accepts iterator of optional values.
    #[inline]
    pub fn extend_values<I, P>(&mut self, iterator: I)
    where
        P: AsRef<[u8]>,
        I: Iterator<Item = P>,
    {
        let length = self.values.len();
        self.values.extend(iterator);
        let additional = self.values.len() - length;

        if let Some(validity) = self.validity.as_mut() {
            validity.extend_constant(additional, true);
        }
    }

    /// Extends the [`MutableBinaryArray`] from an `iterator` of values of trusted length.
    /// This differs from `extend_trusted_len_unchecked` which accepts iterator of optional
    /// values.
    /// # Safety
    /// The `iterator` must be [`TrustedLen`]
    #[inline]
    pub unsafe fn extend_trusted_len_values_unchecked<I, P>(&mut self, iterator: I)
    where
        P: AsRef<[u8]>,
        I: Iterator<Item = P>,
    {
        let length = self.values.len();
        self.values.extend_trusted_len_unchecked(iterator);
        let additional = self.values.len() - length;

        if let Some(validity) = self.validity.as_mut() {
            validity.extend_constant(additional, true);
        }
    }

    /// Extends the [`MutableBinaryArray`] from an iterator of [`TrustedLen`]
    #[inline]
    pub fn extend_trusted_len<I, P>(&mut self, iterator: I)
    where
        P: AsRef<[u8]>,
        I: TrustedLen<Item = Option<P>>,
    {
        // Safety: The iterator is `TrustedLen`
        unsafe { self.extend_trusted_len_unchecked(iterator) }
    }

    /// Extends the [`MutableBinaryArray`] from an iterator of [`TrustedLen`]
    /// # Safety
    /// The `iterator` must be [`TrustedLen`]
    #[inline]
    pub unsafe fn extend_trusted_len_unchecked<I, P>(&mut self, iterator: I)
    where
        P: AsRef<[u8]>,
        I: Iterator<Item = Option<P>>,
    {
        if self.validity.is_none() {
            let mut validity = MutableBitmap::new();
            validity.extend_constant(self.len(), true);
            self.validity = Some(validity);
        }

        self.values
            .extend_from_trusted_len_iter(self.validity.as_mut().unwrap(), iterator);
    }

    /// Creates a new [`MutableBinaryArray`] from a [`Iterator`] of `&[u8]`.
    pub fn from_iter_values<T: AsRef<[u8]>, I: Iterator<Item = T>>(iterator: I) -> Self {
        let (offsets, values) = values_iter(iterator);
        Self::from_data(Self::default_data_type(), offsets, values, None)
    }
}

impl<O: Offset, T: AsRef<[u8]>> Extend<Option<T>> for MutableBinaryArray<O> {
    fn extend<I: IntoIterator<Item = Option<T>>>(&mut self, iter: I) {
        self.try_extend(iter).unwrap();
    }
}

impl<O: Offset, T: AsRef<[u8]>> TryExtend<Option<T>> for MutableBinaryArray<O> {
    fn try_extend<I: IntoIterator<Item = Option<T>>>(&mut self, iter: I) -> Result<()> {
        let mut iter = iter.into_iter();
        self.reserve(iter.size_hint().0, 0);
        iter.try_for_each(|x| self.try_push(x))
    }
}

impl<O: Offset, T: AsRef<[u8]>> TryPush<Option<T>> for MutableBinaryArray<O> {
    fn try_push(&mut self, value: Option<T>) -> Result<()> {
        match value {
            Some(value) => {
                self.values.try_push(value.as_ref())?;

                match &mut self.validity {
                    Some(validity) => validity.push(true),
                    None => {}
                }
            }
            None => {
                self.values.push("");
                match &mut self.validity {
                    Some(validity) => validity.push(false),
                    None => self.init_validity(),
                }
            }
        }
        Ok(())
    }
}

impl<O: Offset> PartialEq for MutableBinaryArray<O> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<O: Offset> TryExtendFromSelf for MutableBinaryArray<O> {
    fn try_extend_from_self(&mut self, other: &Self) -> Result<()> {
        extend_validity(self.len(), &mut self.validity, &other.validity);

        self.values.try_extend_from_self(&other.values)
    }
}
