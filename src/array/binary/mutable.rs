use std::{iter::FromIterator, sync::Arc};

use crate::{
    array::{specification::check_offsets, Array, MutableArray, Offset, TryExtend, TryPush},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::{ArrowError, Result},
    trusted_len::TrustedLen,
};

use super::BinaryArray;
use crate::array::physical_binary::*;

/// The Arrow's equivalent to `Vec<Option<Vec<u8>>>`.
/// Converting a [`MutableBinaryArray`] into a [`BinaryArray`] is `O(1)`.
/// # Implementation
/// This struct does not allocate a validity until one is required (i.e. push a null to it).
#[derive(Debug)]
pub struct MutableBinaryArray<O: Offset> {
    data_type: DataType,
    offsets: Vec<O>,
    values: Vec<u8>,
    validity: Option<MutableBitmap>,
}

impl<O: Offset> From<MutableBinaryArray<O>> for BinaryArray<O> {
    fn from(other: MutableBinaryArray<O>) -> Self {
        BinaryArray::<O>::new(
            other.data_type,
            other.offsets.into(),
            other.values.into(),
            other.validity.map(|x| x.into()),
        )
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

    /// The canonical method to create a [`MutableBinaryArray`] out of low-end APIs.
    /// # Panics
    /// This function panics iff:
    /// * The `offsets` and `values` are inconsistent
    /// * The validity is not `None` and its length is different from `offsets`'s length minus one.
    pub fn from_data(
        data_type: DataType,
        offsets: Vec<O>,
        values: Vec<u8>,
        validity: Option<MutableBitmap>,
    ) -> Self {
        check_offsets(&offsets, values.len());
        if let Some(ref validity) = validity {
            assert_eq!(offsets.len() - 1, validity.len());
        }
        if data_type.to_physical_type() != Self::default_data_type().to_physical_type() {
            panic!("MutableBinaryArray can only be initialized with DataType::Binary or DataType::LargeBinary")
        }
        Self {
            data_type,
            offsets,
            values,
            validity,
        }
    }

    fn default_data_type() -> DataType {
        BinaryArray::<O>::default_data_type()
    }

    /// Creates a new [`MutableBinaryArray`] with capacity for `capacity` values.
    /// # Implementation
    /// This does not allocate the validity.
    pub fn with_capacity(capacity: usize) -> Self {
        let mut offsets = Vec::<O>::with_capacity(capacity + 1);
        offsets.push(O::default());
        Self {
            data_type: BinaryArray::<O>::default_data_type(),
            offsets,
            values: Vec::<u8>::new(),
            validity: None,
        }
    }

    /// Initializes a new [`MutableBinaryArray`] with a pre-allocated capacity of slots and values.
    pub fn with_capacities(capacity: usize, values: usize) -> Self {
        let mut offsets = Vec::<O>::with_capacity(capacity + 1);
        offsets.push(O::default());

        Self {
            data_type: Self::default_data_type(),
            offsets,
            values: Vec::<u8>::with_capacity(values),
            validity: None,
        }
    }

    /// Reserves `additional` slots.
    pub fn reserve(&mut self, additional: usize) {
        self.offsets.reserve(additional);
        if let Some(x) = self.validity.as_mut() {
            x.reserve(additional)
        }
    }

    #[inline]
    fn last_offset(&self) -> O {
        *self.offsets.last().unwrap()
    }

    /// Pushes a new element to the array.
    /// # Panic
    /// This operation panics iff the length of all values (in bytes) exceeds `O` maximum value.
    pub fn push<T: AsRef<[u8]>>(&mut self, value: Option<T>) {
        self.try_push(value).unwrap()
    }

    /// Pop the last entry from [`MutableBinaryArray`].
    /// Note If the values is empty, this method will return None.
    pub fn pop(&mut self) -> Option<Vec<u8>> {
        if self.offsets.len() < 2 {
            return None;
        }
        self.offsets.pop()?;
        let value_start = self.offsets.iter().last().cloned()?.to_usize();
        let value = self.values.split_off(value_start);
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
        let mut validity = MutableBitmap::with_capacity(self.offsets.capacity());
        validity.extend_constant(self.len(), true);
        validity.set(self.len() - 1, false);
        self.validity = Some(validity)
    }

    /// Converts itself into an [`Array`].
    pub fn into_arc(self) -> Arc<dyn Array> {
        let a: BinaryArray<O> = self.into();
        Arc::new(a)
    }
    /// Shrinks the capacity of the [`MutableBinaryArray`] to fit its current length.
    pub fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit();
        self.offsets.shrink_to_fit();
        if let Some(validity) = &mut self.validity {
            validity.shrink_to_fit()
        }
    }
}

impl<O: Offset> MutableBinaryArray<O> {
    /// returns its values.
    pub fn values(&self) -> &Vec<u8> {
        &self.values
    }

    /// returns its offsets.
    pub fn offsets(&self) -> &Vec<O> {
        &self.offsets
    }
}

impl<O: Offset> MutableArray for MutableBinaryArray<O> {
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        self.validity.as_ref()
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(BinaryArray::new(
            self.data_type.clone(),
            std::mem::take(&mut self.offsets).into(),
            std::mem::take(&mut self.values).into(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        ))
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(BinaryArray::new(
            self.data_type.clone(),
            std::mem::take(&mut self.offsets).into(),
            std::mem::take(&mut self.values).into(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        ))
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
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

        if validity.as_mut().unwrap().null_count() == 0 {
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
        let (_, upper) = iterator.size_hint();
        let additional = upper.expect("extend_trusted_len_values requires an upper limit");

        extend_from_trusted_len_values_iter(&mut self.offsets, &mut self.values, iterator);

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

        extend_from_trusted_len_iter(
            &mut self.offsets,
            &mut self.values,
            self.validity.as_mut().unwrap(),
            iterator,
        );

        if self.validity.as_mut().unwrap().null_count() == 0 {
            self.validity = None;
        }
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
        self.reserve(iter.size_hint().0);
        iter.try_for_each(|x| self.try_push(x))
    }
}

impl<O: Offset, T: AsRef<[u8]>> TryPush<Option<T>> for MutableBinaryArray<O> {
    fn try_push(&mut self, value: Option<T>) -> Result<()> {
        match value {
            Some(value) => {
                let bytes = value.as_ref();

                let size =
                    O::from_usize(self.values.len() + bytes.len()).ok_or(ArrowError::Overflow)?;

                self.values.extend_from_slice(bytes);

                self.offsets.push(size);

                match &mut self.validity {
                    Some(validity) => validity.push(true),
                    None => {}
                }
            }
            None => {
                self.offsets.push(self.last_offset());
                match &mut self.validity {
                    Some(validity) => validity.push(false),
                    None => self.init_validity(),
                }
            }
        }
        Ok(())
    }
}
