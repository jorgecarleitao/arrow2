use std::{
    fmt::Binary,
    ops::{BitAnd, BitAndAssign, BitOr, Not, Shl, ShlAssign, ShrAssign},
};

/// Something that can be use as a chunk of bits.
/// Currently implemented for `u8` and `u64`
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
    fn one() -> Self;
    fn zero() -> Self;
    fn to_le(self) -> Self;
}

unsafe impl BitChunk for u8 {
    #[inline(always)]
    fn zero() -> Self {
        0
    }

    #[inline(always)]
    fn to_le(self) -> Self {
        self.to_le()
    }

    #[inline(always)]
    fn one() -> Self {
        1
    }
}

unsafe impl BitChunk for u16 {
    #[inline(always)]
    fn zero() -> Self {
        0
    }

    #[inline(always)]
    fn to_le(self) -> Self {
        self.to_le()
    }

    #[inline(always)]
    fn one() -> Self {
        1
    }
}

unsafe impl BitChunk for u32 {
    #[inline(always)]
    fn zero() -> Self {
        0
    }

    #[inline(always)]
    fn to_le(self) -> Self {
        self.to_le()
    }

    #[inline(always)]
    fn one() -> Self {
        1
    }
}

unsafe impl BitChunk for u64 {
    #[inline(always)]
    fn zero() -> Self {
        0
    }

    #[inline(always)]
    fn to_le(self) -> Self {
        self.to_le()
    }

    #[inline(always)]
    fn one() -> Self {
        1
    }
}

pub struct BitChunkIter<T: BitChunk> {
    value: T,
    mask: T,
    remaining: usize,
}

impl<T: BitChunk> BitChunkIter<T> {
    #[inline]
    pub fn new(value: T) -> Self {
        Self {
            value,
            remaining: std::mem::size_of::<T>() * 8,
            mask: T::one(),
        }
    }
}

impl<T: BitChunk> Iterator for BitChunkIter<T> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            debug_assert_eq!(self.mask, T::zero());
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
    fn test_basic() {
        let a = 0b00010000u8;
        let iter = BitChunkIter::new(a);
        let r = iter.collect::<Vec<_>>();
        assert_eq!(
            r,
            vec![false, false, false, false, true, false, false, false]
        );
    }

    #[test]
    fn test_basic1() {
        let a = [0b00000001, 0b00010000]; // 0th and 13th entry
        let a = u16::from_ne_bytes(a);
        let iter = BitChunkIter::new(a);
        let r = iter.collect::<Vec<_>>();
        assert_eq!(r, (0..16).map(|x| x == 0 || x == 12).collect::<Vec<_>>(),);
    }
}
