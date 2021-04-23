mod merge;

pub use crate::types::BitChunk;
use merge::merge_reversed;

#[derive(Debug)]
pub struct BitChunks<'a, T> {
    buffer: &'a [u8],
    /// offset inside a byte
    bit_offset: u32,
    remainder_bytes_len: usize,
    len: usize,
    phantom: std::marker::PhantomData<T>,
}

#[inline]
fn remainder_bytes(offset: usize, len: usize, bits_in_chunk: usize) -> usize {
    (((offset + len) % bits_in_chunk) + 7) / 8
}

/// writes `bytes` into `dst` assuming that they correspond to an arrow bitmap
/// # Safety
/// `dst` must be writable for `bytes.len()` bytes.
#[inline]
unsafe fn copy_with_merge(mut dst: *mut u8, bytes: &[u8], bit_offset: u32) {
    bytes.windows(2).for_each(|w| {
        let val = merge_reversed(w[0], w[1], bit_offset);
        dst.write(val);
        dst = dst.offset(1);
    });
    let mut last = bytes[bytes.len() - 1];
    last >>= bit_offset;
    dst.write(last);
}

impl<'a, T: BitChunk> BitChunks<'a, T> {
    #[inline]
    fn bits_in_chunk() -> usize {
        std::mem::size_of::<T>() * 8
    }

    pub fn new(buffer: &'a [u8], offset: usize, len: usize) -> Self {
        assert!(offset + len <= buffer.len() * 8);

        let bits_in_chunk = Self::bits_in_chunk();
        let skip_offset = offset / 8;
        let bit_offset = offset % 8;
        let remainder_bytes_len = remainder_bytes(bit_offset, len, bits_in_chunk);
        debug_assert!(bit_offset < 8);

        Self {
            buffer: &buffer[skip_offset..],
            len,
            remainder_bytes_len,
            bit_offset: bit_offset as u32,
            phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn remainder(&self) -> T {
        // remaining bytes may not fit in `size_of::<T>()`. We complement
        // them to fit by allocating T and writing to it byte by byte
        let mut remainder = T::zero();

        match (self.remainder_bytes_len == 0, self.bit_offset == 0) {
            (true, _) => remainder,
            (false, true) => {
                // remaining bytes but not bit offset: bytes bit-aligned
                // and thus we only need to write them to `remainder`.
                let chunked_bytes_len = std::mem::size_of::<T>() * self.chunk_len();

                // all remaining bytes
                let remainder_bytes =
                    &self.buffer[chunked_bytes_len..chunked_bytes_len + self.remainder_bytes_len];

                let mut dst = &mut remainder as *mut T as *mut u8;
                remainder_bytes.iter().for_each(|val| unsafe {
                    dst.write(*val);
                    dst = dst.offset(1);
                });

                remainder
            }
            (false, false) => {
                // there is a min-alignment; we need to write to `remainder`
                // with valid bit-alignment, via `copy_with_merge`
                let dst = &mut remainder as *mut T as *mut u8;

                // build T by combining the remaining bytes at the end.
                // when: len == 514 and T = u64 (=512 bits)
                // chunked_bytes_len = 8 * (514/8) / 8 = 64;
                let chunked_bytes_len =
                    std::mem::size_of::<T>() * (self.len / Self::bits_in_chunk());

                // all remaining bytes
                let remainder_bytes =
                    &self.buffer[chunked_bytes_len..chunked_bytes_len + self.remainder_bytes_len];

                unsafe { copy_with_merge(dst, remainder_bytes, self.bit_offset) };
                remainder
            }
        }
        .to_le()
    }

    #[inline]
    pub fn chunk_len(&self) -> usize {
        self.len / Self::bits_in_chunk()
    }

    /// Returns an iterator over chunks of 64 bits represented as an u64
    #[inline]
    pub fn iter(&self) -> BitChunkIterator<'a, T> {
        BitChunkIterator::<'a, T> {
            buffer: self.buffer,
            offset: self.bit_offset,
            chunk_len: self.chunk_len(),
            chunk_index: 0,
            phantom: self.phantom,
        }
    }

    // in bits
    #[inline]
    pub fn remainder_len(&self) -> usize {
        self.len - (std::mem::size_of::<T>() * ((self.len / 8) / std::mem::size_of::<T>()) * 8)
    }
}

impl<'a, T: BitChunk> IntoIterator for BitChunks<'a, T> {
    type Item = T;
    type IntoIter = BitChunkIterator<'a, T>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Debug)]
pub struct BitChunkIterator<'a, T> {
    buffer: &'a [u8],
    chunk_len: usize,
    offset: u32,
    chunk_index: usize,
    phantom: std::marker::PhantomData<T>,
}

impl<T: BitChunk> Iterator for BitChunkIterator<'_, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        if self.chunk_index >= self.chunk_len {
            return None;
        }

        // cast to *const u64 should be fine since we are using read_unaligned below
        #[allow(clippy::cast_ptr_alignment)]
        let chunk_data = self.buffer.as_ptr() as *const T;

        // bit-packed buffers are stored starting with the least-significant byte first
        // so when reading as u64 on a big-endian machine, the bytes need to be swapped
        let combined = if self.offset == 0 {
            unsafe { std::ptr::read_unaligned(chunk_data.add(self.chunk_index)) }
        } else if self.chunk_index + 1 < self.chunk_len {
            // on the middle we can use the whole next chunk
            let current =
                unsafe { std::ptr::read_unaligned(chunk_data.add(self.chunk_index)) }.to_le();
            let chunk_data = self.buffer.as_ptr() as *const T;
            let next = unsafe { std::ptr::read_unaligned(chunk_data.add(self.chunk_index + 1)) };

            merge_reversed(current, next, self.offset)
        } else {
            // on the last chunk the "next" chunk may not be complete and thus we must create it from remaining bytes.
            let mut remainder = T::zero();
            let dst = &mut remainder as *mut T as *mut u8;

            let size_of = std::mem::size_of::<T>();
            let remainder_bytes =
                &self.buffer[self.chunk_index * size_of..(self.chunk_index + 1) * size_of];

            unsafe { copy_with_merge(dst, remainder_bytes, self.offset) };
            remainder
        };

        self.chunk_index += 1;

        Some(combined.to_le())
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.chunk_len - self.chunk_index,
            Some(self.chunk_len - self.chunk_index),
        )
    }
}

impl<T: BitChunk> ExactSizeIterator for BitChunkIterator<'_, T> {
    #[inline]
    fn len(&self) -> usize {
        self.chunk_len - self.chunk_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bla() {
        assert_eq!(remainder_bytes(2, 2, 64), 1);
        assert_eq!(remainder_bytes(2, 3, 64), 1);
        assert_eq!(remainder_bytes(2, 6, 64), 1);
        assert_eq!(remainder_bytes(2, 8, 64), 2);
    }
}
