use std::iter::FromIterator;

use crate::trusted_len::TrustedLen;

use super::{BooleanArray, MutableBooleanArray};

impl BooleanArray {
    /// Creates a new [`BooleanArray`] from an [`TrustedLen`] of `bool`.
    #[inline]
    pub fn from_trusted_len_values_iter<I: TrustedLen<Item = bool>>(iterator: I) -> Self {
        MutableBooleanArray::from_trusted_len_values_iter(iterator).into()
    }

    /// Creates a new [`BooleanArray`] from a slice of `bool`.
    #[inline]
    pub fn from_slice<P: AsRef<[bool]>>(slice: P) -> Self {
        MutableBooleanArray::from_slice(slice).into()
    }

    /// Creates a [`BooleanArray`] from an iterator of trusted length.
    /// Use this over [`BooleanArray::from_trusted_len_iter`] when the iterator is trusted len
    /// but this crate does not mark it as such.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn from_trusted_len_iter_unchecked<I, P>(iterator: I) -> Self
    where
        P: std::borrow::Borrow<bool>,
        I: Iterator<Item = Option<P>>,
    {
        MutableBooleanArray::from_trusted_len_iter_unchecked(iterator).into()
    }

    /// Creates a [`BooleanArray`] from a [`TrustedLen`].
    #[inline]
    pub fn from_trusted_len_iter<I, P>(iterator: I) -> Self
    where
        P: std::borrow::Borrow<bool>,
        I: TrustedLen<Item = Option<P>>,
    {
        MutableBooleanArray::from_trusted_len_iter(iterator).into()
    }

    /// Creates a [`BooleanArray`] from an falible iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn try_from_trusted_len_iter_unchecked<E, I, P>(iterator: I) -> Result<Self, E>
    where
        P: std::borrow::Borrow<bool>,
        I: Iterator<Item = Result<Option<P>, E>>,
    {
        Ok(MutableBooleanArray::try_from_trusted_len_iter_unchecked(iterator)?.into())
    }

    /// Creates a [`BooleanArray`] from a [`TrustedLen`].
    #[inline]
    pub fn try_from_trusted_len_iter<E, I, P>(iterator: I) -> Result<Self, E>
    where
        P: std::borrow::Borrow<bool>,
        I: TrustedLen<Item = Result<Option<P>, E>>,
    {
        Ok(MutableBooleanArray::try_from_trusted_len_iter(iterator)?.into())
    }
}

impl<Ptr: std::borrow::Borrow<Option<bool>>> FromIterator<Ptr> for BooleanArray {
    fn from_iter<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
        MutableBooleanArray::from_iter(iter).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Array;
    use crate::error::Result;

    #[test]
    fn from_trusted_len_iter() -> Result<()> {
        let iter = std::iter::repeat(true).take(2).map(Some);
        let a = BooleanArray::from_trusted_len_iter(iter);
        assert_eq!(a.len(), 2);
        Ok(())
    }

    #[test]
    fn from_iter() -> Result<()> {
        let iter = std::iter::repeat(true).take(2).map(Some);
        let a = BooleanArray::from_iter(iter);
        assert_eq!(a.len(), 2);
        Ok(())
    }
}
