use std::iter::FromIterator;

use crate::{array::Offset, trusted_len::TrustedLen};

use super::{BinaryArray, MutableBinaryArray};

impl<O: Offset> BinaryArray<O> {
    /// Creates a new [`BinaryArray`] from slices of `&[u8]`.
    pub fn from_slice<T: AsRef<[u8]>, P: AsRef<[T]>>(slice: P) -> Self {
        Self::from_trusted_len_values_iter(slice.as_ref().iter())
    }

    /// Creates a new [`BinaryArray`] from a slice of optional `&[u8]`.
    // Note: this can't be `impl From` because Rust does not allow double `AsRef` on it.
    pub fn from<T: AsRef<[u8]>, P: AsRef<[Option<T>]>>(slice: P) -> Self {
        Self::from_trusted_len_iter(slice.as_ref().iter().map(|x| x.as_ref()))
    }

    /// Creates a [`BinaryArray`] from an iterator of trusted length.
    #[inline]
    pub fn from_trusted_len_values_iter<T: AsRef<[u8]>, I: TrustedLen<Item = T>>(
        iterator: I,
    ) -> Self {
        MutableBinaryArray::<O>::from_trusted_len_values_iter(iterator).into()
    }

    /// Creates a new [`BinaryArray`] from a [`Iterator`] of `&str`.
    pub fn from_iter_values<T: AsRef<[u8]>, I: Iterator<Item = T>>(iterator: I) -> Self {
        MutableBinaryArray::<O>::from_iter_values(iterator).into()
    }
}

impl<O: Offset, P: AsRef<[u8]>> FromIterator<Option<P>> for BinaryArray<O> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        MutableBinaryArray::from_iter(iter).into()
    }
}

impl<O: Offset> BinaryArray<O> {
    /// Creates a [`BinaryArray`] from an iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn from_trusted_len_iter_unchecked<I, P>(iterator: I) -> Self
    where
        P: AsRef<[u8]>,
        I: Iterator<Item = Option<P>>,
    {
        MutableBinaryArray::<O>::from_trusted_len_iter_unchecked(iterator).into()
    }

    /// Creates a [`BinaryArray`] from an iterator of trusted length.
    #[inline]
    pub fn from_trusted_len_iter<I, P>(iterator: I) -> Self
    where
        P: AsRef<[u8]>,
        I: TrustedLen<Item = Option<P>>,
    {
        // soundness: I is `TrustedLen`
        unsafe { Self::from_trusted_len_iter_unchecked(iterator) }
    }

    /// Creates a [`BinaryArray`] from an falible iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn try_from_trusted_len_iter_unchecked<E, I, P>(iterator: I) -> Result<Self, E>
    where
        P: AsRef<[u8]>,
        I: IntoIterator<Item = Result<Option<P>, E>>,
    {
        MutableBinaryArray::<O>::try_from_trusted_len_iter_unchecked(iterator).map(|x| x.into())
    }

    /// Creates a [`BinaryArray`] from an fallible iterator of trusted length.
    #[inline]
    pub fn try_from_trusted_len_iter<E, I, P>(iter: I) -> Result<Self, E>
    where
        P: AsRef<[u8]>,
        I: TrustedLen<Item = Result<Option<P>, E>>,
    {
        // soundness: I: TrustedLen
        unsafe { Self::try_from_trusted_len_iter_unchecked(iter) }
    }
}
