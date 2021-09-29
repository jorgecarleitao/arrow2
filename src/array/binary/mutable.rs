use std::{iter::FromIterator, sync::Arc};

use crate::{
    array::{specification::check_offsets, Array, MutableArray, Offset, TryExtend, TryPush},
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
    datatypes::DataType,
    error::{ArrowError, Result},
    trusted_len::TrustedLen,
};

use super::BinaryArray;

/// The Arrow's equivalent to `Vec<Option<Vec<u8>>>`.
/// Converting a [`MutableBinaryArray`] into a [`BinaryArray`] is `O(1)`.
/// # Implementation
/// This struct does not allocate a validity until one is required (i.e. push a null to it).
#[derive(Debug)]
pub struct MutableBinaryArray<O: Offset> {
    data_type: DataType,
    offsets: MutableBuffer<O>,
    values: MutableBuffer<u8>,
    validity: Option<MutableBitmap>,
}

impl<O: Offset> From<MutableBinaryArray<O>> for BinaryArray<O> {
    fn from(other: MutableBinaryArray<O>) -> Self {
        BinaryArray::<O>::from_data(
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
    /// This allocates a [`MutableBuffer`] of one element
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
        offsets: MutableBuffer<O>,
        values: MutableBuffer<u8>,
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
        let mut offsets = MutableBuffer::<O>::with_capacity(capacity + 1);
        offsets.push(O::default());
        Self {
            data_type: BinaryArray::<O>::default_data_type(),
            offsets,
            values: MutableBuffer::<u8>::new(),
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
        self.validity = Some(MutableBitmap::from_trusted_len_iter(
            std::iter::repeat(true)
                .take(self.len() - 1)
                .chain(std::iter::once(false)),
        ))
    }

    /// Converts itself into an [`Array`].
    pub fn into_arc(self) -> Arc<dyn Array> {
        let a: BinaryArray<O> = self.into();
        Arc::new(a)
    }
    /// Shrinks the capacity of the [`MutableBinaryArray`] to fit its current length.
    pub fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit();
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
        Box::new(BinaryArray::from_data(
            self.data_type.clone(),
            std::mem::take(&mut self.offsets).into(),
            std::mem::take(&mut self.values).into(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        ))
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(BinaryArray::from_data(
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
    #[inline]
    pub fn from_trusted_len_values_iter<T: AsRef<[u8]>, I: TrustedLen<Item = T>>(
        iterator: I,
    ) -> Self {
        // soundness: I is `TrustedLen`
        let (offsets, values) = unsafe { trusted_len_values_iter(iterator) };
        Self::from_data(Self::default_data_type(), offsets, values, None)
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
        let (validity, offsets, values) = try_trusted_len_unzip(iterator)?;

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

                let size = O::from_usize(self.values.len() + bytes.len())
                    .ok_or(ArrowError::KeyOverflowError)?;

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
    P: AsRef<[u8]>,
    I: Iterator<Item = Option<P>>,
{
    let (_, upper) = iterator.size_hint();
    let len = upper.expect("trusted_len_unzip requires an upper limit");

    let mut null = MutableBitmap::with_capacity(len);
    let mut offsets = MutableBuffer::<O>::with_capacity(len + 1);
    let mut values = MutableBuffer::<u8>::new();

    let mut length = O::default();
    let mut dst = offsets.as_mut_ptr();
    std::ptr::write(dst, length);
    dst = dst.add(1);
    for item in iterator {
        if let Some(item) = item {
            null.push(true);
            let s = item.as_ref();
            length += O::from_usize(s.len()).unwrap();
            values.extend_from_slice(s);
        } else {
            null.push(false);
            values.extend_from_slice(b"");
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

    (null.into(), offsets, values)
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
    P: AsRef<[u8]>,
    I: Iterator<Item = std::result::Result<Option<P>, E>>,
{
    let (_, upper) = iterator.size_hint();
    let len = upper.expect("trusted_len_unzip requires an upper limit");

    let mut null = MutableBitmap::with_capacity(len);
    let mut offsets = MutableBuffer::<O>::with_capacity(len + 1);
    let mut values = MutableBuffer::<u8>::new();

    let mut length = O::default();
    let mut dst = offsets.as_mut_ptr();
    std::ptr::write(dst, length);
    dst = dst.add(1);
    for item in iterator {
        if let Some(item) = item? {
            null.push(true);
            let s = item.as_ref();
            length += O::from_usize(s.len()).unwrap();
            values.extend_from_slice(s);
        } else {
            null.push(false);
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

    Ok((null.into(), offsets, values))
}

/// Creates two [`Buffer`]s from an iterator of `&[u8]`.
/// The first buffer corresponds to a offset buffer, the second to a values buffer.
/// # Safety
/// The caller must ensure that `iterator` is [`TrustedLen`].
#[inline]
pub(crate) unsafe fn trusted_len_values_iter<O, I, P>(
    iterator: I,
) -> (MutableBuffer<O>, MutableBuffer<u8>)
where
    O: Offset,
    P: AsRef<[u8]>,
    I: Iterator<Item = P>,
{
    let (_, upper) = iterator.size_hint();
    let len = upper.expect("trusted_len_unzip requires an upper limit");

    let mut offsets = MutableBuffer::<O>::with_capacity(len + 1);
    let mut values = MutableBuffer::<u8>::new();

    let mut length = O::default();
    let mut dst = offsets.as_mut_ptr();
    std::ptr::write(dst, length);
    dst = dst.add(1);
    for item in iterator {
        let s = item.as_ref();
        length += O::from_usize(s.len()).unwrap();
        values.extend_from_slice(s);

        std::ptr::write(dst, length);
        dst = dst.add(1);
    }
    assert_eq!(
        dst.offset_from(offsets.as_ptr()) as usize,
        len + 1,
        "Trusted iterator length was not accurately reported"
    );
    offsets.set_len(len + 1);

    (offsets, values)
}

/// Creates two [`MutableBuffer`]s from an iterator of `&[u8]`.
/// The first buffer corresponds to a offset buffer, the second to a values buffer.
#[inline]
fn values_iter<O, I, P>(iterator: I) -> (MutableBuffer<O>, MutableBuffer<u8>)
where
    O: Offset,
    P: AsRef<[u8]>,
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
        values.extend_from_slice(s);

        offsets.push(length)
    }
    (offsets, values)
}
