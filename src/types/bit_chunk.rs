use std::{
    fmt::Binary,
    ops::{BitAnd, BitAndAssign, BitOr, Not, Shl, ShlAssign, ShrAssign},
};

/// Something that can be use as a chunk of bits. This is used to create masks of a given number
/// of lengths, whose width is `1`. In `simd_packed` notation, this corresponds to `m1xY`.
/// # Safety
/// Do not implement.
pub unsafe trait BitChunk:
    Sized
    + Copy
    + std::fmt::Debug
    + Binary
    + BitAnd<Output = Self>
    + ShlAssign
    + Not<Output = Self>
    + ShrAssign<u32>
    + ShlAssign<u32>
    + Shl<u32, Output = Self>
    + Eq
    + BitAndAssign
    + BitOr<Output = Self>
{
    type Bytes: std::ops::Index<usize, Output = u8>
        + std::ops::IndexMut<usize, Output = u8>
        + for<'a> std::convert::TryFrom<&'a [u8]>
        + std::fmt::Debug;

    fn one() -> Self;
    fn zero() -> Self;
    fn to_ne_bytes(self) -> Self::Bytes;
    fn from_ne_bytes(v: Self::Bytes) -> Self;
}

unsafe impl BitChunk for u8 {
    type Bytes = [u8; 1];

    #[inline(always)]
    fn zero() -> Self {
        0
    }

    #[inline(always)]
    fn to_ne_bytes(self) -> Self::Bytes {
        self.to_ne_bytes()
    }

    #[inline(always)]
    fn from_ne_bytes(v: Self::Bytes) -> Self {
        Self::from_ne_bytes(v)
    }

    #[inline(always)]
    fn one() -> Self {
        1
    }
}

unsafe impl BitChunk for u16 {
    type Bytes = [u8; 2];

    #[inline(always)]
    fn zero() -> Self {
        0
    }

    #[inline(always)]
    fn to_ne_bytes(self) -> Self::Bytes {
        self.to_ne_bytes()
    }

    #[inline(always)]
    fn from_ne_bytes(v: Self::Bytes) -> Self {
        Self::from_ne_bytes(v)
    }

    #[inline(always)]
    fn one() -> Self {
        1
    }
}

unsafe impl BitChunk for u32 {
    type Bytes = [u8; 4];

    #[inline(always)]
    fn zero() -> Self {
        0
    }

    #[inline(always)]
    fn from_ne_bytes(v: Self::Bytes) -> Self {
        Self::from_ne_bytes(v)
    }

    #[inline(always)]
    fn to_ne_bytes(self) -> Self::Bytes {
        self.to_ne_bytes()
    }

    #[inline(always)]
    fn one() -> Self {
        1
    }
}

unsafe impl BitChunk for u64 {
    type Bytes = [u8; 8];

    #[inline(always)]
    fn zero() -> Self {
        0
    }

    #[inline(always)]
    fn to_ne_bytes(self) -> Self::Bytes {
        self.to_ne_bytes()
    }

    #[inline(always)]
    fn from_ne_bytes(v: Self::Bytes) -> Self {
        Self::from_ne_bytes(v)
    }

    #[inline(always)]
    fn one() -> Self {
        1
    }
}

/// An iterator of `bool` over a [`BitChunk`]. This iterator is often
/// compiled to SIMD instructions.
/// The [LSB](https://en.wikipedia.org/wiki/Bit_numbering#Least_significant_bit) corresponds
/// to the first slot, as defined by the arrow specification.
/// # Example
/// ```
/// # use arrow2::types::BitChunkIter;
/// let a = 0b00010000u8;
/// let iter = BitChunkIter::new(a, 7);
/// let r = iter.collect::<Vec<_>>();
/// assert_eq!(r, vec![false, false, false, false, true, false, false]);
/// ```
pub struct BitChunkIter<T: BitChunk> {
    value: T,
    mask: T,
    remaining: usize,
}

impl<T: BitChunk> BitChunkIter<T> {
    #[inline]
    pub fn new(value: T, len: usize) -> Self {
        assert!(len <= std::mem::size_of::<T>() * 8);
        Self {
            value,
            remaining: len,
            mask: T::one(),
        }
    }
}

impl<T: BitChunk> Iterator for BitChunkIter<T> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        };
        let result = Some(self.value & self.mask != T::zero());
        self.remaining -= 1;
        self.mask <<= 1;
        result
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic1() {
        let a = [0b00000001, 0b00010000]; // 0th and 13th entry
        let a = u16::from_ne_bytes(a);
        let iter = BitChunkIter::new(a, 16);
        let r = iter.collect::<Vec<_>>();
        assert_eq!(r, (0..16).map(|x| x == 0 || x == 12).collect::<Vec<_>>(),);
    }
}
