use std::iter::FromIterator;

use crate::{
    array::Offset,
    bitmap::{Bitmap, MutableBitmap},
    buffer::{Buffer, MutableBuffer},
    trusted_len::TrustedLen,
};

use super::{BinaryArray, MutableBinaryArray};

impl<O: Offset> BinaryArray<O> {
    pub fn from_slice<T: AsRef<[u8]>, P: AsRef<[T]>>(slice: P) -> Self {
        Self::from_iter(slice.as_ref().iter().map(Some))
    }

    /// Creates a new [`BinaryArray`] from a slice of `&[u8]`.
    // Note: this can't be `impl From` because Rust does not allow double `AsRef` on it.
    pub fn from<T: AsRef<[u8]>, P: AsRef<[Option<T>]>>(slice: P) -> Self {
        Self::from_trusted_len_iter(slice.as_ref().iter().map(|x| x.as_ref()))
    }

    /// Creates a [`BinaryArray`] from an iterator of trusted length.
    #[inline]
    pub fn from_trusted_len_iter<I, P>(iterator: I) -> Self
    where
        P: AsRef<[u8]>,
        I: TrustedLen<Item = Option<P>>,
    {
        // soundness: I is `TrustedLen`
        let (validity, offsets, values) = unsafe { trusted_len_unzip(iterator) };

        Self::from_data(offsets, values, validity)
    }
}

impl<O: Offset, P: AsRef<[u8]>> FromIterator<Option<P>> for BinaryArray<O> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        MutableBinaryArray::from_iter(iter).into()
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

    (null.into(), offsets.into(), values.into())
}

#[cfg(test)]
mod tests {
    use crate::array::Array;

    use super::*;

    #[test]
    fn test_from() {
        let array = BinaryArray::<i32>::from(&[Some(b"hello".as_ref()), Some(b" ".as_ref()), None]);

        let a = array.validity().as_ref().unwrap();
        assert_eq!(a.len(), 3);
        assert_eq!(a.as_slice()[0], 0b00000011);
    }
}
