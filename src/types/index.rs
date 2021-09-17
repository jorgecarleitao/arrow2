use std::convert::TryFrom;

use crate::{
    trusted_len::TrustedLen,
    types::{NativeType, NaturalDataType},
};

/// iterator of [`Index`] equivalent to `(a..b)`.
// `Step` is unstable in Rust which does not allow (a..b) for generic `Index`.
pub struct IndexRange<I: Index> {
    start: I,
    end: I,
}

impl<I: Index> IndexRange<I> {
    /// Returns a new [`IndexRange`].
    pub fn new(start: I, end: I) -> Self {
        assert!(end >= start);
        Self { start, end }
    }
}

impl<I: Index> Iterator for IndexRange<I> {
    type Item = I;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.start == self.end {
            return None;
        }
        let old = self.start;
        self.start += I::one();
        Some(old)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = (self.end - self.start).to_usize();
        (len, Some(len))
    }
}

/// Safety: a range is always of known length
unsafe impl<I: Index> TrustedLen for IndexRange<I> {}

/// Types that can be used to index a slot of an array.
pub trait Index:
    NativeType
    + NaturalDataType
    + std::ops::AddAssign
    + std::ops::Sub<Output = Self>
    + num_traits::One
    + PartialOrd
{
    /// Convert itself to [`usize`].
    fn to_usize(&self) -> usize;
    /// Convert itself from [`usize`].
    fn from_usize(index: usize) -> Option<Self>;

    /// An iterator from (inclusive) `start` to (exclusive) `end`.
    fn range(start: usize, end: usize) -> Option<IndexRange<Self>> {
        let start = Self::from_usize(start);
        let end = Self::from_usize(end);
        match (start, end) {
            (Some(start), Some(end)) => Some(IndexRange::new(start, end)),
            _ => None,
        }
    }
}

impl Index for i32 {
    #[inline]
    fn to_usize(&self) -> usize {
        *self as usize
    }

    #[inline]
    fn from_usize(value: usize) -> Option<Self> {
        Self::try_from(value).ok()
    }
}

impl Index for i64 {
    #[inline]
    fn to_usize(&self) -> usize {
        *self as usize
    }

    #[inline]
    fn from_usize(value: usize) -> Option<Self> {
        Self::try_from(value).ok()
    }
}

impl Index for u32 {
    #[inline]
    fn to_usize(&self) -> usize {
        *self as usize
    }

    #[inline]
    fn from_usize(value: usize) -> Option<Self> {
        Self::try_from(value).ok()
    }
}

impl Index for u64 {
    #[inline]
    fn to_usize(&self) -> usize {
        *self as usize
    }

    #[inline]
    fn from_usize(value: usize) -> Option<Self> {
        Self::try_from(value).ok()
    }
}
