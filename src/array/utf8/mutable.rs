use std::{iter::FromIterator, sync::Arc};

use crate::{
    array::{
        specification::{check_offsets_and_utf8, check_offsets_minimal},
        Array, MutableArray, Offset, TryExtend, TryPush,
    },
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
    datatypes::DataType,
    error::{ArrowError, Result},
    trusted_len::TrustedLen,
};

use super::Utf8Array;

/// The mutable version of [`Utf8Array`]. See [`MutableArray`] for more details.
#[derive(Debug)]
pub struct MutableUtf8Array<O: Offset> {
    data_type: DataType,
    offsets: MutableBuffer<O>,
    values: MutableBuffer<u8>,
    validity: Option<MutableBitmap>,
}

impl<O: Offset> From<MutableUtf8Array<O>> for Utf8Array<O> {
    fn from(other: MutableUtf8Array<O>) -> Self {
        // Safety:
        // `MutableUtf8Array` has the same invariants as `Utf8Array` and thus
        // `Utf8Array` can be safely created from `MutableUtf8Array` without checks.
        unsafe {
            Utf8Array::<O>::from_data_unchecked(
                other.data_type,
                other.offsets.into(),
                other.values.into(),
                other.validity.map(|x| x.into()),
            )
        }
    }
}

impl<O: Offset> Default for MutableUtf8Array<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Offset> MutableUtf8Array<O> {
    /// Initializes a new empty [`MutableUtf8Array`].
    pub fn new() -> Self {
        let mut offsets = MutableBuffer::<O>::new();
        offsets.push(O::default());
        Self {
            data_type: Self::default_data_type(),
            offsets,
            values: MutableBuffer::<u8>::new(),
            validity: None,
        }
    }

    /// The canonical method to create a [`MutableUtf8Array`] out of low-end APIs.
    /// # Panics
    /// This function panics iff:
    /// * The `offsets` and `values` are inconsistent
    /// * The `values` between `offsets` are not utf8 encoded
    /// * The validity is not `None` and its length is different from `offsets`'s length minus one.
    pub fn from_data(
        data_type: DataType,
        offsets: MutableBuffer<O>,
        values: MutableBuffer<u8>,
        validity: Option<MutableBitmap>,
    ) -> Self {
        check_offsets_and_utf8(&offsets, &values);
        if let Some(ref validity) = validity {
            assert_eq!(offsets.len() - 1, validity.len());
        }
        if data_type.to_physical_type() != Self::default_data_type().to_physical_type() {
            panic!("MutableUtf8Array can only be initialized with DataType::Utf8 or DataType::LargeUtf8")
        }
        Self {
            data_type,
            offsets,
            values,
            validity,
        }
    }

    /// Create a [`MutableUtf8Array`] out of low-end APIs.
    /// # Safety
    /// The caller must ensure that every value between offsets is a valid utf8.
    /// # Panics
    /// This function panics iff:
    /// * The `offsets` and `values` are inconsistent
    /// * The validity is not `None` and its length is different from `offsets`'s length minus one.
    pub unsafe fn from_data_unchecked(
        data_type: DataType,
        offsets: MutableBuffer<O>,
        values: MutableBuffer<u8>,
        validity: Option<MutableBitmap>,
    ) -> Self {
        check_offsets_minimal(&offsets, values.len());
        if let Some(ref validity) = validity {
            assert_eq!(offsets.len() - 1, validity.len());
        }
        if data_type.to_physical_type() != Self::default_data_type().to_physical_type() {
            panic!("MutableUtf8Array can only be initialized with DataType::Utf8 or DataType::LargeUtf8")
        }
        Self {
            data_type,
            offsets,
            values,
            validity,
        }
    }

    fn default_data_type() -> DataType {
        Utf8Array::<O>::default_data_type()
    }

    /// Initializes a new [`MutableUtf8Array`] with a pre-allocated capacity of slots.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacities(capacity, 0)
    }

    /// Initializes a new [`MutableUtf8Array`] with a pre-allocated capacity of slots and values.
    pub fn with_capacities(capacity: usize, values: usize) -> Self {
        let mut offsets = MutableBuffer::<O>::with_capacity(capacity + 1);
        offsets.push(O::default());

        Self {
            data_type: Self::default_data_type(),
            offsets,
            values: MutableBuffer::<u8>::with_capacity(values),
            validity: None,
        }
    }

    /// Reserves `additional` elements and `additional_values` on the values buffer.
    pub fn reserve(&mut self, additional: usize, additional_values: usize) {
        self.offsets.reserve(additional);
        if let Some(x) = self.validity.as_mut() {
            x.reserve(additional)
        }
        self.values.reserve(additional_values);
    }

    #[inline]
    fn last_offset(&self) -> O {
        *self.offsets.last().unwrap()
    }

    /// Pushes a new element to the array.
    /// # Panic
    /// This operation panics iff the length of all values (in bytes) exceeds `O` maximum value.
    pub fn push<T: AsRef<str>>(&mut self, value: Option<T>) {
        self.try_push(value).unwrap()
    }

    fn init_validity(&mut self) {
        let mut validity = MutableBitmap::with_capacity(self.offsets.capacity());
        validity.extend_constant(self.len(), true);
        validity.set(self.len() - 1, false);
        self.validity = Some(validity);
    }

    /// Converts itself into an [`Array`].
    pub fn into_arc(self) -> Arc<dyn Array> {
        let a: Utf8Array<O> = self.into();
        Arc::new(a)
    }

    /// Shrinks the capacity of the [`MutableUtf8Array`] to fit its current length.
    pub fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit();
        self.offsets.shrink_to_fit();
        if let Some(validity) = &mut self.validity {
            validity.shrink_to_fit()
        }
    }
}

impl<O: Offset> MutableUtf8Array<O> {
    /// returns its values.
    pub fn values(&self) -> &MutableBuffer<u8> {
        &self.values
    }

    /// returns its offsets.
    pub fn offsets(&self) -> &MutableBuffer<O> {
        &self.offsets
    }
}

impl<O: Offset> MutableArray for MutableUtf8Array<O> {
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        self.validity.as_ref()
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(Utf8Array::from_data(
            Self::default_data_type(),
            std::mem::take(&mut self.offsets).into(),
            std::mem::take(&mut self.values).into(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        ))
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(Utf8Array::from_data(
            Self::default_data_type(),
            std::mem::take(&mut self.offsets).into(),
            std::mem::take(&mut self.values).into(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        ))
    }

    fn data_type(&self) -> &DataType {
        if O::is_large() {
            &DataType::LargeUtf8
        } else {
            &DataType::Utf8
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn push_null(&mut self) {
        self.push::<&str>(None)
    }

    fn shrink_to_fit(&mut self) {
        self.shrink_to_fit()
    }
}

impl<O: Offset, P: AsRef<str>> FromIterator<Option<P>> for MutableUtf8Array<O> {
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        Self::try_from_iter(iter).unwrap()
    }
}

impl<O: Offset> MutableUtf8Array<O> {
    /// Extends the [`MutableUtf8Array`] from an iterator of values of trusted len.
    /// This differs from `extended_trusted_len` which accepts iterator of optional values.
    #[inline]
    pub fn extend_trusted_len_values<I, P>(&mut self, iterator: I)
    where
        P: AsRef<str>,
        I: TrustedLen<Item = P>,
    {
        unsafe { self.extend_trusted_len_values_unchecked(iterator) }
    }

    /// Extends the [`MutableUtf8Array`] from an iterator of values of trusted len.
    /// This differs from `extended_trusted_len_unchecked` which accepts iterator of optional
    /// values.
    /// # Safety
    /// The iterator must be trusted len.
    #[inline]
    pub unsafe fn extend_trusted_len_values_unchecked<I, P>(&mut self, iterator: I)
    where
        P: AsRef<str>,
        I: Iterator<Item = P>,
    {
        let (_, upper) = iterator.size_hint();
        let additional = upper.expect("extend_trusted_len_values requires an upper limit");

        extend_from_trusted_len_values_iter(&mut self.offsets, &mut self.values, iterator);

        if let Some(validity) = self.validity.as_mut() {
            validity.extend_constant(additional, true);
        }
    }

    /// Extends the [`MutableUtf8Array`] from an iterator of trusted len.
    #[inline]
    pub fn extend_trusted_len<I, P>(&mut self, iterator: I)
    where
        P: AsRef<str>,
        I: TrustedLen<Item = Option<P>>,
    {
        unsafe { self.extend_trusted_len_unchecked(iterator) }
    }

    /// Extends [`MutableUtf8Array`] from an iterator of trusted len.
    /// # Safety
    /// The iterator must be trusted len.
    #[inline]
    pub unsafe fn extend_trusted_len_unchecked<I, P>(&mut self, iterator: I)
    where
        P: AsRef<str>,
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

    /// Creates a [`MutableUtf8Array`] from an iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn from_trusted_len_iter_unchecked<I, P>(iterator: I) -> Self
    where
        P: AsRef<str>,
        I: Iterator<Item = Option<P>>,
    {
        let (validity, offsets, values) = trusted_len_unzip(iterator);

        // soundness: P is `str`
        Self::from_data_unchecked(Self::default_data_type(), offsets, values, validity)
    }

    /// Creates a [`MutableUtf8Array`] from an iterator of trusted length.
    #[inline]
    pub fn from_trusted_len_iter<I, P>(iterator: I) -> Self
    where
        P: AsRef<str>,
        I: TrustedLen<Item = Option<P>>,
    {
        // soundness: I is `TrustedLen`
        unsafe { Self::from_trusted_len_iter_unchecked(iterator) }
    }

    /// Creates a [`MutableUtf8Array`] from an iterator of trusted length of `&str`.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn from_trusted_len_values_iter_unchecked<T: AsRef<str>, I: Iterator<Item = T>>(
        iterator: I,
    ) -> Self {
        let (offsets, values) = unsafe { trusted_len_values_iter(iterator) };
        // soundness: T is AsRef<str>
        Self::from_data_unchecked(Self::default_data_type(), offsets, values, None)
    }

    /// Creates a new [`Utf8Array`] from a [`TrustedLen`] of `&str`.
    #[inline]
    pub fn from_trusted_len_values_iter<T: AsRef<str>, I: TrustedLen<Item = T>>(
        iterator: I,
    ) -> Self {
        // soundness: I is `TrustedLen`
        unsafe { Self::from_trusted_len_values_iter_unchecked(iterator) }
    }

    /// Creates a new [`MutableUtf8Array`] from an iterator.
    /// # Error
    /// This operation errors iff the total length in bytes on the iterator exceeds `O`'s maximum value.
    /// (`i32::MAX` or `i64::MAX` respectively).
    fn try_from_iter<P: AsRef<str>, I: IntoIterator<Item = Option<P>>>(iter: I) -> Result<Self> {
        let iterator = iter.into_iter();
        let (lower, _) = iterator.size_hint();
        let mut array = Self::with_capacity(lower);
        for item in iterator {
            array.try_push(item)?;
        }
        Ok(array)
    }

    /// Creates a [`MutableUtf8Array`] from an falible iterator of trusted length.
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
        let iterator = iterator.into_iter();

        // soundness: assumed trusted len
        let (validity, offsets, values) = try_trusted_len_unzip(iterator)?;

        // soundness: P is `str`
        Ok(Self::from_data_unchecked(
            Self::default_data_type(),
            offsets,
            values,
            validity,
        ))
    }

    /// Creates a [`MutableUtf8Array`] from an falible iterator of trusted length.
    #[inline]
    pub fn try_from_trusted_len_iter<E, I, P>(iterator: I) -> std::result::Result<Self, E>
    where
        P: AsRef<str>,
        I: TrustedLen<Item = std::result::Result<Option<P>, E>>,
    {
        // soundness: I: TrustedLen
        unsafe { Self::try_from_trusted_len_iter_unchecked(iterator) }
    }

    /// Creates a new [`MutableUtf8Array`] from a [`Iterator`] of `&str`.
    pub fn from_iter_values<T: AsRef<str>, I: Iterator<Item = T>>(iterator: I) -> Self {
        let (offsets, values) = values_iter(iterator);
        // soundness: T: AsRef<str>
        unsafe { Self::from_data_unchecked(Self::default_data_type(), offsets, values, None) }
    }
}

impl<O: Offset, T: AsRef<str>> Extend<Option<T>> for MutableUtf8Array<O> {
    fn extend<I: IntoIterator<Item = Option<T>>>(&mut self, iter: I) {
        self.try_extend(iter).unwrap();
    }
}

impl<O: Offset, T: AsRef<str>> TryExtend<Option<T>> for MutableUtf8Array<O> {
    fn try_extend<I: IntoIterator<Item = Option<T>>>(&mut self, iter: I) -> Result<()> {
        let mut iter = iter.into_iter();
        self.reserve(iter.size_hint().0, 0);
        iter.try_for_each(|x| self.try_push(x))
    }
}

impl<O: Offset, T: AsRef<str>> TryPush<Option<T>> for MutableUtf8Array<O> {
    fn try_push(&mut self, value: Option<T>) -> Result<()> {
        match value {
            Some(value) => {
                let bytes = value.as_ref().as_bytes();
                self.values.extend_from_slice(bytes);

                let size = O::from_usize(self.values.len()).ok_or(ArrowError::KeyOverflowError)?;

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

/// Creates [`MutableBitmap`] and two [`MutableBuffer`]s from an iterator of `Option`.
/// The first buffer corresponds to a offset buffer, the second one
/// corresponds to a values buffer.
/// # Safety
/// The caller must ensure that `iterator` is `TrustedLen`.
#[inline]
unsafe fn trusted_len_unzip<O, I, P>(
    iterator: I,
) -> (Option<MutableBitmap>, MutableBuffer<O>, MutableBuffer<u8>)
where
    O: Offset,
    P: AsRef<str>,
    I: Iterator<Item = Option<P>>,
{
    let mut offsets = MutableBuffer::<O>::with_capacity(1);
    let mut values = MutableBuffer::<u8>::new();
    let mut validity = MutableBitmap::new();

    offsets.push_unchecked(O::default());

    extend_from_trusted_len_iter(&mut offsets, &mut values, &mut validity, iterator);

    let validity = if validity.null_count() > 0 {
        Some(validity)
    } else {
        None
    };

    (validity, offsets, values)
}

/// # Safety
/// The caller must ensure that `iterator` is `TrustedLen`.
#[inline]
#[allow(clippy::type_complexity)]
pub(crate) unsafe fn try_trusted_len_unzip<E, I, P, O>(
    iterator: I,
) -> std::result::Result<(Option<MutableBitmap>, MutableBuffer<O>, MutableBuffer<u8>), E>
where
    O: Offset,
    P: AsRef<str>,
    I: Iterator<Item = std::result::Result<Option<P>, E>>,
{
    let (_, upper) = iterator.size_hint();
    let len = upper.expect("trusted_len_unzip requires an upper limit");

    let mut validity = MutableBitmap::with_capacity(len);
    let mut offsets = MutableBuffer::<O>::with_capacity(len + 1);
    let mut values = MutableBuffer::<u8>::new();

    let mut length = O::default();
    let mut dst = offsets.as_mut_ptr();
    std::ptr::write(dst, length);
    dst = dst.add(1);
    for item in iterator {
        if let Some(item) = item? {
            validity.push(true);
            let s = item.as_ref();
            length += O::from_usize(s.len()).unwrap();
            values.extend_from_slice(s.as_bytes());
        } else {
            validity.push(false);
        };
        std::ptr::write(dst, length);
        dst = dst.add(1);
    }
    assert_eq!(
        dst.offset_from(offsets.as_ptr()) as usize,
        len + 1,
        "Trusted iterator length was not accurately reported"
    );
    offsets.set_len(len + 1);

    let validity = if validity.null_count() > 0 {
        Some(validity)
    } else {
        None
    };

    Ok((validity, offsets, values))
}

/// Creates two [`Buffer`]s from an iterator of `&str`.
/// The first buffer corresponds to a offset buffer, the second to a values buffer.
/// # Safety
/// The caller must ensure that `iterator` is [`TrustedLen`].
#[inline]
pub(crate) unsafe fn trusted_len_values_iter<O, I, P>(
    iterator: I,
) -> (MutableBuffer<O>, MutableBuffer<u8>)
where
    O: Offset,
    P: AsRef<str>,
    I: Iterator<Item = P>,
{
    let mut offsets = MutableBuffer::<O>::with_capacity(1 + iterator.size_hint().1.unwrap());
    let mut values = MutableBuffer::<u8>::new();

    offsets.push_unchecked(O::default());

    extend_from_trusted_len_values_iter(&mut offsets, &mut values, iterator);

    (offsets, values)
}

/// Populates `offsets` and `values` [`Buffer`] with information
/// extracted from the incoming iterator.
/// # Safety
/// The caller must ensure that `iterator` is [`TrustedLen`]
#[inline]
unsafe fn extend_from_trusted_len_values_iter<I, P, O>(
    offsets: &mut MutableBuffer<O>,
    values: &mut MutableBuffer<u8>,
    iterator: I,
) where
    O: Offset,
    P: AsRef<str>,
    I: Iterator<Item = P>,
{
    let (_, upper) = iterator.size_hint();
    let additional = upper.expect("extend_from_trusted_len_iter_values requires an upper limit");

    offsets.reserve(additional);

    let mut length = *offsets.last().unwrap();

    let mut dst = offsets.as_mut_ptr();
    dst = dst.add(offsets.len());

    for item in iterator {
        let s = item.as_ref();

        length += O::from_usize(s.len()).unwrap();

        values.extend_from_slice(s.as_bytes());
        std::ptr::write(dst, length);

        dst = dst.add(1);
    }

    assert_eq!(
        dst.offset_from(offsets.as_ptr()) as usize,
        offsets.len() + additional,
        "Trusted iterator length was not accurately reported"
    );

    offsets.set_len(offsets.len() + additional);
}

/// Populates `offsets`, `values`, and validity [`Buffer`] with information
/// extracted from the incoming iterator.
/// # Safety
/// The caller must ensure that `iterator` is [`TrustedLen`]
#[inline]
unsafe fn extend_from_trusted_len_iter<O, I, P>(
    offsets: &mut MutableBuffer<O>,
    values: &mut MutableBuffer<u8>,
    validity: &mut MutableBitmap,
    iterator: I,
) where
    O: Offset,
    P: AsRef<str>,
    I: Iterator<Item = Option<P>>,
{
    let (_, upper) = iterator.size_hint();
    let additional = upper.expect("extend_from_trusted_len_values_iter requires an upper limit");

    offsets.reserve(additional);
    validity.reserve(additional);

    let mut length = *offsets.last().unwrap();

    let mut dst = offsets.as_mut_ptr();
    dst = dst.add(offsets.len());

    for item in iterator {
        if let Some(item) = item {
            let s = item.as_ref();

            length += O::from_usize(s.len()).unwrap();

            values.extend_from_slice(s.as_bytes());
            validity.push_unchecked(true);
        } else {
            validity.push_unchecked(false);
        };

        std::ptr::write(dst, length);

        dst = dst.add(1);
    }

    assert_eq!(
        dst.offset_from(offsets.as_ptr()) as usize,
        offsets.len() + additional,
        "Trusted iterator length was not accurately reported"
    );

    offsets.set_len(offsets.len() + additional);
}

/// Creates two [`MutableBuffer`]s from an iterator of `&str`.
/// The first buffer corresponds to a offset buffer, the second to a values buffer.
#[inline]
fn values_iter<O, I, P>(iterator: I) -> (MutableBuffer<O>, MutableBuffer<u8>)
where
    O: Offset,
    P: AsRef<str>,
    I: Iterator<Item = P>,
{
    let (lower, _) = iterator.size_hint();

    let mut offsets = MutableBuffer::<O>::with_capacity(lower + 1);
    let mut values = MutableBuffer::<u8>::new();

    let mut length = O::default();
    offsets.push(length);

    for item in iterator {
        let s = item.as_ref();
        length += O::from_usize(s.len()).unwrap();
        values.extend_from_slice(s.as_bytes());

        offsets.push(length)
    }
    (offsets, values)
}
