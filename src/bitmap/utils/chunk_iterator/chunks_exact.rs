use std::{convert::TryInto, slice::ChunksExact};

use super::{BitChunk, BitChunkIterExact};

/// An iterator over a [`BitChunk`] from a slice of bytes.
#[derive(Debug)]
pub struct BitChunksExact<'a, T: BitChunk> {
    iter: ChunksExact<'a, u8>,
    remainder: &'a [u8],
    phantom: std::marker::PhantomData<T>,
}

impl<'a, T: BitChunk> BitChunksExact<'a, T> {
    #[inline]
    pub fn new(slice: &'a [u8], len: usize) -> Self {
        let size_of = std::mem::size_of::<T>();
        let bytes_len = (len + 7) / 8;
        let (iter, remainder) = if size_of != 1 {
            // case where a chunk has more than one byte
            let chunks = slice.chunks_exact(size_of);
            let remainder_bytes = chunks.remainder();
            (chunks, remainder_bytes)
        } else {
            // case where a chunk is exactly one byte
            let chunks = &slice[..len / 8];
            let chunks = chunks.chunks_exact(size_of);
            (chunks, &slice[slice.len() - 1..bytes_len])
        };

        Self {
            iter,
            remainder,
            phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.iter.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn remainder(&self) -> T {
        let remainder_bytes = self.remainder;
        if remainder_bytes.is_empty() {
            return T::zero();
        }
        let remainder = match remainder_bytes.try_into() {
            Ok(a) => a,
            Err(_) => {
                let mut remainder = T::zero().to_ne_bytes();
                remainder_bytes
                    .iter()
                    .enumerate()
                    .for_each(|(index, b)| remainder[index] = *b);
                remainder
            }
        };
        T::from_ne_bytes(remainder)
    }
}

impl<T: BitChunk> Iterator for BitChunksExact<'_, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|x| match x.try_into() {
            Ok(a) => T::from_ne_bytes(a),
            Err(_) => unreachable!(),
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<T: BitChunk> BitChunkIterExact<T> for BitChunksExact<'_, T> {
    #[inline]
    fn remainder(&self) -> T {
        self.remainder()
    }
}
