use crate::{
    buffer::{types::NativeType, Bitmap, Buffer, MutableBitmap, MutableBuffer},
    datatypes::DataType,
};

use super::PrimitiveArray;

impl<T: NativeType> PrimitiveArray<T> {
    pub fn from_slice<P: AsRef<[T]>>((datatype, slice): (DataType, P)) -> Self {
        unsafe { Self::from_trusted_len_iter(datatype, slice.as_ref().iter().map(Some)) }
    }
}

impl<T: NativeType> PrimitiveArray<T> {
    /// Creates a [`PrimitiveArray`] from an iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn from_trusted_len_iter<I, P>(data_type: DataType, iter: I) -> Self
    where
        P: std::borrow::Borrow<T>,
        I: IntoIterator<Item = Option<P>>,
    {
        let iterator = iter.into_iter();

        let (validity, values) = trusted_len_unzip(iterator);

        Self::from_data(data_type, values, validity)
    }

    /// Creates a [`PrimitiveArray`] from an falible iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn try_from_trusted_len_iter<E, I, P>(
        data_type: DataType,
        iter: I,
    ) -> Result<Self, E>
    where
        P: std::borrow::Borrow<T>,
        I: IntoIterator<Item = Result<Option<P>, E>>,
    {
        let iterator = iter.into_iter();

        let (validity, values) = try_trusted_len_unzip(iterator)?;

        Ok(Self::from_data(data_type, values, validity))
    }
}

/// Creates a Bitmap and a [`Buffer`] from an iterator of `Option`.
/// The first buffer corresponds to a bitmap buffer, the second one
/// corresponds to a values buffer.
/// # Safety
/// The caller must ensure that `iterator` is `TrustedLen`.
#[inline]
pub(crate) unsafe fn trusted_len_unzip<I, P, T>(iterator: I) -> (Option<Bitmap>, Buffer<T>)
where
    T: NativeType,
    P: std::borrow::Borrow<T>,
    I: Iterator<Item = Option<P>>,
{
    let (_, upper) = iterator.size_hint();
    let len = upper.expect("trusted_len_unzip requires an upper limit");

    let mut null = MutableBitmap::with_capacity(len);
    let mut buffer = MutableBuffer::<T>::with_capacity(len);

    let mut dst = buffer.as_mut_ptr();
    for item in iterator {
        let item = if let Some(item) = item {
            null.push_unchecked(true);
            *item.borrow()
        } else {
            null.push_unchecked(false);
            T::default()
        };
        std::ptr::write(dst, item);
        dst = dst.add(1);
    }
    assert_eq!(
        dst.offset_from(buffer.as_ptr()) as usize,
        len,
        "Trusted iterator length was not accurately reported"
    );
    buffer.set_len(len);
    null.set_len(len);

    let bitmap = if null.null_count() > 0 {
        Some(null.into())
    } else {
        None
    };
    (bitmap, buffer.into())
}

/// # Safety
/// The caller must ensure that `iterator` is `TrustedLen`.
#[inline]
pub(crate) unsafe fn try_trusted_len_unzip<E, I, P, T>(
    iterator: I,
) -> Result<(Option<Bitmap>, Buffer<T>), E>
where
    T: NativeType,
    P: std::borrow::Borrow<T>,
    I: Iterator<Item = Result<Option<P>, E>>,
{
    let (_, upper) = iterator.size_hint();
    let len = upper.expect("trusted_len_unzip requires an upper limit");

    let mut null = MutableBitmap::with_capacity(len);
    let mut buffer = MutableBuffer::<T>::with_capacity(len);

    let mut dst = buffer.as_mut_ptr();
    for item in iterator {
        let item = if let Some(item) = item? {
            null.push_unchecked(true);
            *item.borrow()
        } else {
            null.push_unchecked(false);
            T::default()
        };
        std::ptr::write(dst, item);
        dst = dst.add(1);
    }
    assert_eq!(
        dst.offset_from(buffer.as_ptr()) as usize,
        len,
        "Trusted iterator length was not accurately reported"
    );
    buffer.set_len(len);
    null.set_len(len);

    let bitmap = if null.null_count() > 0 {
        Some(null.into())
    } else {
        None
    };
    Ok((bitmap, buffer.into()))
}
