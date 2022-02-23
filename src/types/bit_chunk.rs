use std::{
    fmt::Binary,
    ops::{BitAnd, BitAndAssign, BitOr, Not, Shl, ShlAssign, ShrAssign},
};

use super::NativeType;

/// A chunk of bits. This is used to create masks of a given length
/// whose width is `1` bit. In `simd_packed` notation, this corresponds to `m1xY`.
pub trait BitChunk:
    super::private::Sealed
    + NativeType
    + Binary
    + BitAnd<Output = Self>
    + ShlAssign
    + Not<Output = Self>
    + ShrAssign<usize>
    + ShlAssign<usize>
    + Shl<usize, Output = Self>
    + Eq
    + BitAndAssign
    + BitOr<Output = Self>
{
    /// A value with a single bit set at the most right position.
    fn one() -> Self;
    /// A value with no bits set.
    fn zero() -> Self;
    /// convert itself into bytes.
    fn to_ne_bytes(self) -> Self::Bytes;
    /// convert itself from bytes.
    fn from_ne_bytes(v: Self::Bytes) -> Self;
}

macro_rules! bit_chunk {
    ($ty:ty) => {
        impl BitChunk for $ty {
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
    };
}

bit_chunk!(u8);
bit_chunk!(u16);
bit_chunk!(u32);
bit_chunk!(u64);

/// An [`Iterator<Item=bool>`] over a [`BitChunk`]. This iterator is often
/// compiled to SIMD.
/// The [LSB](https://en.wikipedia.org/wiki/Bit_numbering#Least_significant_bit) corresponds
/// to the first slot, as defined by the arrow specification.
/// # Example
/// ```
/// use arrow2::types::BitChunkIter;
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
    /// Creates a new [`BitChunkIter`] with `len` bits.
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
