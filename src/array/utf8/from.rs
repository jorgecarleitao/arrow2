use std::iter::FromIterator;

use crate::array::Offset;
use crate::trusted_len::TrustedLen;

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
        MutableUtf8Array::<O>::from_trusted_len_values_iter(iterator).into()
    }

    /// Creates a new [`Utf8Array`] from a [`Iterator`] of `&str`.
    pub fn from_iter_values<T: AsRef<str>, I: Iterator<Item = T>>(iterator: I) -> Self {
        MutableUtf8Array::<O>::from_iter_values(iterator).into()
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
        MutableUtf8Array::<O>::from_trusted_len_iter_unchecked(iterator).into()
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

    /// Creates a [`Utf8Array`] from an falible iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn try_from_trusted_len_iter_unchecked<E, I, P>(iterator: I) -> Result<Self, E>
    where
        P: AsRef<str>,
        I: IntoIterator<Item = Result<Option<P>, E>>,
    {
        MutableUtf8Array::<O>::try_from_trusted_len_iter_unchecked(iterator).map(|x| x.into())
    }

    /// Creates a [`Utf8Array`] from an fallible iterator of trusted length.
    #[inline]
    pub fn try_from_trusted_len_iter<E, I, P>(iter: I) -> Result<Self, E>
    where
        P: AsRef<str>,
        I: TrustedLen<Item = Result<Option<P>, E>>,
    {
        // soundness: I: TrustedLen
        unsafe { Self::try_from_trusted_len_iter_unchecked(iter) }
    }
}

impl<O: Offset, P: AsRef<str>> FromIterator<Option<P>> for Utf8Array<O> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        MutableUtf8Array::from_iter(iter).into()
    }
}
