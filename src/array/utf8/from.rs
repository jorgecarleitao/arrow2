use std::iter::FromIterator;

use crate::trusted_len::TrustedLen;
use crate::{
    array::Offset,
    bitmap::{Bitmap, MutableBitmap},
    buffer::{Buffer, MutableBuffer},
};

use super::{MutableUtf8Array, Utf8Array};

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

    /// Creates a new [`Utf8Array`] from a [`Iterator`] of `&str`.
    pub fn from_iter_values<T: AsRef<str>, I: IntoIterator<Item = T>>(iter: I) -> Self {
        let iterator = iter.into_iter();
        let (offsets, values) = values_iter(iterator);
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

/// Creates two [`Buffer`]s from an iterator of `&str`.
/// The first buffer corresponds to a offset buffer, the second to a values buffer.
#[inline]
fn values_iter<O, I, P>(iterator: I) -> (Buffer<O>, Buffer<u8>)
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

impl<O: Offset, P: AsRef<str>> FromIterator<Option<P>> for Utf8Array<O> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        MutableUtf8Array::from_iter(iter).into()
    }
}

#[cfg(test)]
mod tests {
    use crate::array::Array;

    use super::*;

    #[test]
    fn test_from() {
        let array = Utf8Array::<i32>::from(&[Some("hello"), Some(" "), None]);

        let a = array.validity().as_ref().unwrap();
        assert_eq!(a.len(), 3);
        assert_eq!(a.as_slice()[0], 0b00000011);
    }

    #[test]
    fn test_from_iter_values() {
        let b = Utf8Array::<i32>::from_iter_values(vec!["a", "b", "cc"]);

        let offsets = Buffer::from(&[0, 1, 2, 4]);
        let values = Buffer::from("abcc".as_bytes());
        assert_eq!(b, Utf8Array::<i32>::from_data(offsets, values, None));
    }
}
