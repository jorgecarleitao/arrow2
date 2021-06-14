use std::{iter::FromIterator, sync::Arc};

use crate::{
    array::{Array, Builder, IntoArray, Offset, ToArray, TryFromIterator},
    bitmap::{Bitmap, MutableBitmap},
    buffer::{Buffer, MutableBuffer},
    datatypes::DataType,
};
use crate::{
    error::{ArrowError, Result as ArrowResult},
    trusted_len::TrustedLen,
};

use super::Utf8Array;

impl<O: Offset> Utf8Array<O> {
    /// Creates a new [`Utf8Array`] from a slice of `&str`.
    /// This is a convenience method that just calls [`Self::from_trusted_len_values_iter`].
    #[inline]
    pub fn from_slice<T: AsRef<str>, P: AsRef<[T]>>(slice: P) -> Self {
        Self::from_trusted_len_values_iter(slice.as_ref().iter())
    }

    /// Creates a new [`Utf8Array`] from a slice of `&str`.
    // Note: this can't be `impl From` because Rust does not allow double `AsRef` on it.
    pub fn from<T: AsRef<str>, P: AsRef<[Option<T>]>>(slice: P) -> Self {
        Self::from_trusted_len_iter(slice.as_ref().iter().map(|x| x.as_ref()))
    }

    /// Creates a new [`Utf8Array`] from a [`TrustedLen`] of `&str`.
    #[inline]
    pub fn from_trusted_len_values_iter<T: AsRef<str>, I: TrustedLen<Item = T>>(
        iterator: I,
    ) -> Self {
        let (offsets, values) = unsafe { trusted_len_values_iter(iterator) };
        Self::from_data(offsets, values, None)
    }
}

impl<O: Offset> Utf8Array<O> {
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
        let (validity, offsets, values) = trusted_len_unzip(iterator);

        // soundness: P is `str`
        Self::from_data_unchecked(offsets, values, validity)
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

    /// Creates a [`PrimitiveArray`] from an falible iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn try_from_trusted_len_iter<E, I, P>(iter: I) -> Result<Self, E>
    where
        P: AsRef<str>,
        I: IntoIterator<Item = Result<Option<P>, E>>,
    {
        let iterator = iter.into_iter();

        let (validity, offsets, values) = try_trusted_len_unzip(iterator)?;

        // soundness: P is `str`
        Ok(Self::from_data_unchecked(offsets, values, validity))
    }
}

/// Creates [`Bitmap`] and two [`Buffer`]s from an iterator of `Option`.
/// The first buffer corresponds to a offset buffer, the second one
/// corresponds to a values buffer.
/// # Safety
/// The caller must ensure that `iterator` is `TrustedLen`.
#[inline]
pub(crate) unsafe fn trusted_len_unzip<O, I, P>(
    iterator: I,
) -> (Option<Bitmap>, Buffer<O>, Buffer<u8>)
where
    O: Offset,
    P: AsRef<str>,
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
            values.extend_from_slice(s.as_bytes());
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

    (null.into(), offsets.into(), values.into())
}

/// Creates two [`Buffer`]s from an iterator of `&str`.
/// The first buffer corresponds to a offset buffer, the second to a values buffer.
/// # Safety
/// The caller must ensure that `iterator` is [`TrustedLen`].
#[inline]
pub(crate) unsafe fn trusted_len_values_iter<O, I, P>(iterator: I) -> (Buffer<O>, Buffer<u8>)
where
    O: Offset,
    P: AsRef<str>,
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
        values.extend_from_slice(s.as_bytes());

        std::ptr::write(dst, length);
        dst = dst.add(1);
    }
    assert_eq!(
        dst.offset_from(offsets.as_ptr()) as usize,
        len + 1,
        "Trusted iterator length was not accurately reported"
    );
    offsets.set_len(len + 1);

    (offsets.into(), values.into())
}

/// # Safety
/// The caller must ensure that `iterator` is `TrustedLen`.
#[inline]
#[allow(clippy::type_complexity)]
pub(crate) unsafe fn try_trusted_len_unzip<E, I, P, O>(
    iterator: I,
) -> Result<(Option<Bitmap>, Buffer<O>, Buffer<u8>), E>
where
    O: Offset,
    P: AsRef<str>,
    I: Iterator<Item = Result<Option<P>, E>>,
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
            values.extend_from_slice(s.as_bytes());
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

    Ok((null.into(), offsets.into(), values.into()))
}

/// auxiliary struct used to create a [`PrimitiveArray`] out of an iterator
#[derive(Debug)]
pub struct Utf8Primitive<O: Offset> {
    offsets: MutableBuffer<O>,
    values: MutableBuffer<u8>,
    validity: MutableBitmap,
    // invariant: always equal to the last offset
    length: O,
}

impl<O: Offset> Utf8Primitive<O> {
    /// Initializes a new [`Utf8Primitive`] with a pre-allocated capacity of slots.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacities(capacity, 0)
    }

    /// Initializes a new [`Utf8Primitive`] with a pre-allocated capacity of slots and values.
    pub fn with_capacities(capacity: usize, values: usize) -> Self {
        let mut offsets = MutableBuffer::<O>::with_capacity(capacity + 1);
        let length = O::default();
        offsets.push(length);

        Self {
            offsets,
            values: MutableBuffer::<u8>::with_capacity(values),
            validity: MutableBitmap::with_capacity(capacity),
            length,
        }
    }
}

impl<O: Offset, P: AsRef<str>> FromIterator<Option<P>> for Utf8Primitive<O> {
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        Self::try_from_iter(iter.into_iter().map(Ok)).unwrap()
    }
}

impl<O: Offset, P: AsRef<str>> FromIterator<Option<P>> for Utf8Array<O> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        Utf8Primitive::from_iter(iter).to()
    }
}

impl<O: Offset, P> TryFromIterator<Option<P>> for Utf8Primitive<O>
where
    P: AsRef<str>,
{
    fn try_from_iter<I: IntoIterator<Item = ArrowResult<Option<P>>>>(iter: I) -> ArrowResult<Self> {
        let iterator = iter.into_iter();
        let (lower, _) = iterator.size_hint();
        let mut primitive = Self::with_capacity(lower);
        for item in iterator {
            match item? {
                Some(x) => primitive.try_push(Some(&x.as_ref()))?,
                None => primitive.try_push(None)?,
            }
        }
        Ok(primitive)
    }
}

impl<O: Offset> Builder<&str> for Utf8Primitive<O> {
    fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    #[inline]
    fn try_push(&mut self, value: Option<&str>) -> ArrowResult<()> {
        match value {
            Some(v) => {
                let bytes = v.as_bytes();
                let length =
                    O::from_usize(bytes.len()).ok_or(ArrowError::DictionaryKeyOverflowError)?;
                self.length += length;
                self.offsets.push(self.length);
                self.values.extend_from_slice(bytes);
                self.validity.push(true);
            }
            None => {
                self.offsets.push(self.length);
                self.validity.push(false);
            }
        }
        Ok(())
    }

    #[inline]
    fn push(&mut self, value: Option<&str>) {
        self.try_push(value).unwrap()
    }
}

impl<O: Offset> Utf8Primitive<O> {
    pub fn to(self) -> Utf8Array<O> {
        // Soundness: all methods from `Utf8Primitive` receive &str
        unsafe {
            Utf8Array::<O>::from_data_unchecked(
                self.offsets.into(),
                self.values.into(),
                self.validity.into(),
            )
        }
    }
}

impl<O: Offset> ToArray for Utf8Primitive<O> {
    fn to_arc(self, _: &DataType) -> Arc<dyn Array> {
        Arc::new(self.to())
    }
}

impl<O: Offset> IntoArray for Utf8Primitive<O> {
    fn into_arc(self) -> Arc<dyn Array> {
        Arc::new(self.to())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from() {
        let array = Utf8Array::<i32>::from(&[Some("hello"), Some(" "), None]);

        let a = array.validity().as_ref().unwrap();
        assert_eq!(a.len(), 3);
        assert_eq!(a.as_slice()[0], 0b00000011);
    }

    #[test]
    fn test_capacities() {
        let b = Utf8Primitive::<i32>::with_capacities(1, 10);

        assert_eq!(b.values.capacity(), 64);
        assert_eq!(b.offsets.capacity(), 16); // 64 bytes
    }
}
