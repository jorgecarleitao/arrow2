use std::convert::TryInto;

mod chunks_exact;
mod merge;

pub use crate::types::BitChunk;
pub use chunks_exact::BitChunksExact;

use crate::{trusted_len::TrustedLen, types::BitChunkIter};
use merge::merge_reversed;

pub trait BitChunkIterExact<B: BitChunk>: Iterator<Item = B> {
    fn remainder(&self) -> B;
}

/// This struct is used to efficiently iterate over bit masks by loading bytes on
/// the stack with alignments of `uX`. This allows efficient iteration over bitmaps.
#[derive(Debug)]
pub struct BitChunks<'a, T: BitChunk> {
    chunk_iterator: std::slice::ChunksExact<'a, u8>,
    current: T,
    remainder_bytes: &'a [u8],
    last_chunk: T,
    remaining: usize,
    /// offset inside a byte
    bit_offset: usize,
    len: usize,
    phantom: std::marker::PhantomData<T>,
}

/// writes `bytes` into `dst`.
#[inline]
fn copy_with_merge<T: BitChunk>(dst: &mut T::Bytes, bytes: &[u8], bit_offset: usize) {
    let mut last = bytes[bytes.len() - 1];
    last >>= bit_offset;
    dst[0] = last;
    bytes
        .windows(2)
        .take(std::mem::size_of::<T>())
        .enumerate()
        .for_each(|(i, w)| {
            let val = merge_reversed(w[0], w[1], bit_offset);
            dst[i] = val;
        });
}

impl<'a, T: BitChunk> BitChunks<'a, T> {
    pub fn new(slice: &'a [u8], offset: usize, len: usize) -> Self {
        assert!(offset + len <= slice.len() * 8);

        let slice = &slice[offset / 8..];
        let bit_offset = offset % 8;
        let size_of = std::mem::size_of::<T>();

        let bytes_len = len / 8;
        let bytes_upper_len = (len + bit_offset + 7) / 8;
        let mut chunks = slice[..bytes_len].chunks_exact(size_of);

        let remainder = &slice[bytes_len - chunks.remainder().len()..bytes_upper_len];

        let remainder_bytes = if chunks.len() == 0 { slice } else { remainder };

        let last_chunk = remainder_bytes
            .first()
            .map(|first| {
                let mut last = T::zero().to_ne_bytes();
                last[0] = *first;
                T::from_ne_bytes(last)
            })
            .unwrap_or_else(T::zero);

        let remaining = chunks.size_hint().0;

        let current = chunks
            .next()
            .map(|x| match x.try_into() {
                Ok(a) => T::from_ne_bytes(a),
                Err(_) => unreachable!(),
            })
            .unwrap_or_else(T::zero);

        Self {
            chunk_iterator: chunks,
            len,
            current,
            remaining,
            remainder_bytes,
            last_chunk,
            bit_offset,
            phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    fn load_next(&mut self) {
        self.current = match self.chunk_iterator.next().unwrap().try_into() {
            Ok(a) => T::from_ne_bytes(a),
            Err(_) => unreachable!(),
        };
    }

    #[inline]
    pub fn remainder(&self) -> T {
        // remaining bytes may not fit in `size_of::<T>()`. We complement
        // them to fit by allocating T and writing to it byte by byte
        let mut remainder = T::zero().to_ne_bytes();

        let remainder = match (self.remainder_bytes.is_empty(), self.bit_offset == 0) {
            (true, _) => remainder,
            (false, true) => {
                // all remaining bytes
                self.remainder_bytes
                    .iter()
                    .take(std::mem::size_of::<T>())
                    .enumerate()
                    .for_each(|(i, val)| remainder[i] = *val);

                remainder
            }
            (false, false) => {
                // all remaining bytes
                copy_with_merge::<T>(&mut remainder, self.remainder_bytes, self.bit_offset);
                remainder
            }
        };
        T::from_ne_bytes(remainder)
    }

    // in bits
    #[inline]
    pub fn remainder_len(&self) -> usize {
        self.len - (std::mem::size_of::<T>() * ((self.len / 8) / std::mem::size_of::<T>()) * 8)
    }

    #[inline]
    pub fn remainder_iter(&self) -> BitChunkIter<T> {
        BitChunkIter::new(self.remainder(), self.remainder_len())
    }
}

impl<T: BitChunk> Iterator for BitChunks<'_, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        if self.remaining == 0 {
            return None;
        }

        let current = self.current;
        let combined = if self.bit_offset == 0 {
            // fast case where there is no offset. In this case, there is bit-alignment
            // at byte boundary and thus the bytes correspond exactly.
            if self.remaining >= 2 {
                self.load_next();
            }
            current
        } else {
            let next = if self.remaining >= 2 {
                // case where `next` is complete and thus we can take it all
                self.load_next();
                self.current
            } else {
                // case where the `next` is incomplete and thus we take the remaining
                self.last_chunk
            };
            merge_reversed(current, next, self.bit_offset)
        };

        self.remaining -= 1;
        Some(combined)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        // it contains always one more than the chunk_iterator, which is the last
        // one where the remainder is merged into current.
        (self.remaining, Some(self.remaining))
    }
}

impl<T: BitChunk> BitChunkIterExact<T> for BitChunks<'_, T> {
    fn remainder(&self) -> T {
        self.remainder()
    }
}

impl<T: BitChunk> ExactSizeIterator for BitChunks<'_, T> {
    #[inline]
    fn len(&self) -> usize {
        self.chunk_iterator.len()
    }
}

unsafe impl<T: BitChunk> TrustedLen for BitChunks<'_, T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basics() {
        let mut iter = BitChunks::<u16>::new(&[0b00000001u8, 0b00000010u8], 0, 16);
        assert_eq!(iter.next().unwrap(), 0b0000_0010_0000_0001u16);
        assert_eq!(iter.remainder(), 0);
    }

    #[test]
    fn remainder() {
        let a = BitChunks::<u16>::new(&[0b00000001u8, 0b00000010u8, 0b00000100u8], 0, 18);
        assert_eq!(a.remainder(), 0b00000100u16);
    }

    #[test]
    fn remainder_saturating() {
        let a = BitChunks::<u16>::new(&[0b00000001u8, 0b00000010u8, 0b00000010u8], 0, 18);
        assert_eq!(a.remainder(), 0b0000_0000_0000_0010u16);
    }

    #[test]
    fn basics_offset() {
        let mut iter = BitChunks::<u16>::new(&[0b00000001u8, 0b00000011u8, 0b00000001u8], 1, 16);
        assert_eq!(iter.remainder(), 0);
        assert_eq!(iter.next().unwrap(), 0b1000_0001_1000_0000u16);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn basics_offset_remainder() {
        let mut a = BitChunks::<u16>::new(&[0b00000001u8, 0b00000011u8, 0b10000001u8], 1, 15);
        assert_eq!(a.next(), None);
        assert_eq!(a.remainder(), 0b1000_0001_1000_0000u16);
        assert_eq!(a.remainder_len(), 15);
    }

    #[test]
    fn offset_remainder_saturating() {
        let a = BitChunks::<u16>::new(&[0b00000001u8, 0b00000011u8, 0b00000011u8], 1, 17);
        assert_eq!(a.remainder(), 0b0000_0000_0000_0001u16);
    }

    #[test]
    fn offset_remainder_saturating2() {
        let a = BitChunks::<u64>::new(&[0b01001001u8, 0b00000001], 1, 8);
        assert_eq!(a.remainder(), 0b1010_0100u64);
    }

    #[test]
    fn offset_remainder_saturating3() {
        let input: &[u8] = &[0b01000000, 0b01000001];
        let a = BitChunks::<u64>::new(input, 8, 2);
        assert_eq!(a.remainder(), 0b0100_0001u64);
    }

    #[test]
    fn basics_multiple() {
        let mut iter = BitChunks::<u16>::new(
            &[0b00000001u8, 0b00000010u8, 0b00000100u8, 0b00001000u8],
            0,
            4 * 8,
        );
        assert_eq!(iter.next().unwrap(), 0b0000_0010_0000_0001u16);
        assert_eq!(iter.next().unwrap(), 0b0000_1000_0000_0100u16);
        assert_eq!(iter.remainder(), 0);
    }

    #[test]
    fn basics_multiple_offset() {
        let mut iter = BitChunks::<u16>::new(
            &[
                0b00000001u8,
                0b00000010u8,
                0b00000100u8,
                0b00001000u8,
                0b00000001u8,
            ],
            1,
            4 * 8,
        );
        assert_eq!(iter.next().unwrap(), 0b0000_0001_0000_0000u16);
        assert_eq!(iter.next().unwrap(), 0b1000_0100_0000_0010u16);
        assert_eq!(iter.remainder(), 0);
    }

    #[test]
    fn remainder_large() {
        let input: &[u8] = &[
            0b00100100, 0b01001001, 0b10010010, 0b00100100, 0b01001001, 0b10010010, 0b00100100,
            0b01001001, 0b10010010, 0b00100100, 0b01001001, 0b10010010, 0b00000100,
        ];
        let mut iter = BitChunks::<u8>::new(input, 0, 8 * 12 + 4);
        assert_eq!(iter.remainder_len(), 100 - 96);

        for j in 0..12 {
            let mut a = BitChunkIter::new(iter.next().unwrap(), 8);
            for i in 0..8 {
                assert_eq!(a.next().unwrap(), (j * 8 + i + 1) % 3 == 0);
            }
        }
        assert_eq!(None, iter.next());

        use crate::types::BitChunkIter;
        let expected_remainder = 0b00000100u8;
        assert_eq!(iter.remainder(), expected_remainder);

        let mut a = BitChunkIter::new(expected_remainder, 8);
        for i in 0..4 {
            assert_eq!(a.next().unwrap(), (i + 1) % 3 == 0);
        }
    }

    #[test]
    fn basics_1() {
        let mut iter = BitChunks::<u16>::new(
            &[0b00000001u8, 0b00000010u8, 0b00000100u8, 0b00001000u8],
            8,
            3 * 8,
        );
        assert_eq!(iter.next().unwrap(), 0b0000_0100_0000_0010u16);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.remainder(), 0b0000_0000_0000_1000u16);
        assert_eq!(iter.remainder_len(), 8);
    }

    #[test]
    fn basics_2() {
        let mut iter = BitChunks::<u16>::new(
            &[0b00000001u8, 0b00000010u8, 0b00000100u8, 0b00001000u8],
            7,
            3 * 8,
        );
        assert_eq!(iter.remainder(), 0b0000_0000_0001_0000u16);
        assert_eq!(iter.next().unwrap(), 0b0000_1000_0000_0100u16);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn remainder_1() {
        let mut iter = BitChunks::<u64>::new(&[0b11111111u8, 0b00000001u8], 0, 9);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.remainder(), 0b1_1111_1111u64);
    }
}
