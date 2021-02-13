use std::sync::Arc;

use crate::{
    array::{Array, Builder, Offset, ToArray},
    buffer::{Bitmap, Buffer, MutableBitmap, MutableBuffer},
    datatypes::DataType,
};

use super::Utf8Array;

impl<O: Offset> Utf8Array<O> {
    pub fn from_slice<T: AsRef<str>, P: AsRef<[T]>>(slice: P) -> Self {
        unsafe { Self::from_trusted_len_iter(slice.as_ref().iter().map(Some)) }
    }
}

impl<O: Offset, T: AsRef<str>> From<&Vec<Option<T>>> for Utf8Array<O> {
    fn from(slice: &Vec<Option<T>>) -> Self {
        unsafe { Self::from_trusted_len_iter(slice.iter().map(|x| x.as_ref())) }
    }
}

impl<O: Offset> Utf8Array<O> {
    /// Creates a [`PrimitiveArray`] from an iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn from_trusted_len_iter<I, P>(iter: I) -> Self
    where
        P: AsRef<str>,
        I: IntoIterator<Item = Option<P>>,
    {
        let iterator = iter.into_iter();

        let (validity, offsets, values) = trusted_len_unzip(iterator);

        // soundness: P is `str`
        Self::from_data_unchecked(offsets, values, validity)
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

/// Creates a Bitmap and a [`Buffer`] from an iterator of `Option`.
/// The first buffer corresponds to a bitmap buffer, the second one
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
            null.push_unchecked(true);
            let s = item.as_ref();
            length += O::from_usize(s.len()).unwrap();
            values.extend_from_slice(s.as_bytes());
        } else {
            null.push_unchecked(false);
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
    null.set_len(len);

    (null.into(), offsets.into(), values.into())
}

/// # Safety
/// The caller must ensure that `iterator` is `TrustedLen`.
#[inline]
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
    offsets.push(O::default());
    let mut values = MutableBuffer::<u8>::new();

    let mut length = O::default();
    let mut dst = offsets.as_mut_ptr();
    for item in iterator {
        if let Some(item) = item? {
            null.push_unchecked(true);
            let s = item.as_ref();
            length += O::from_usize(s.len()).unwrap();
            values.extend_from_slice(s.as_bytes());
        } else {
            null.push_unchecked(false);
        };
        std::ptr::write(dst, length);
        dst = dst.add(1);
    }
    assert_eq!(
        dst.offset_from(offsets.as_ptr()) as usize,
        len,
        "Trusted iterator length was not accurately reported"
    );
    offsets.set_len(len);
    null.set_len(len);

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

impl<O: Offset> Builder<&str> for Utf8Primitive<O> {
    #[inline]
    fn with_capacity(capacity: usize) -> Self {
        let mut offsets = MutableBuffer::<O>::with_capacity(capacity + 1);
        offsets.push(O::default());

        Self {
            offsets,
            values: MutableBuffer::<u8>::new(),
            validity: MutableBitmap::with_capacity(capacity),
            length: O::default(),
        }
    }

    #[inline]
    fn push(&mut self, value: Option<&&str>) {
        match value {
            Some(v) => {
                self.offsets.push(O::from_usize(v.len()).unwrap());
                self.values.extend_from_slice(&v.as_bytes());
                self.validity.push(true);
            }
            None => {
                self.offsets.push(self.length);
                self.validity.push(false);
            }
        }
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
