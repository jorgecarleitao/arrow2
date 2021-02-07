use super::bytes_for;

#[derive(Debug)]
pub struct BitChunks<'a> {
    buffer: &'a [u8],
    /// offset inside a byte, guaranteed to be between 0 and 7 (inclusive)
    bit_offset: usize,
    /// number of complete u64 chunks
    chunk_len: usize,
    /// number of remaining bits, guaranteed to be between 0 and 63 (inclusive)
    remainder_len: usize,
}

impl<'a> BitChunks<'a> {
    pub fn new(buffer: &'a [u8], offset: usize, len: usize) -> Self {
        assert!(offset + len <= buffer.len() * 8);

        let byte_offset = offset / 8;
        let bit_offset = offset % 8;

        let chunk_bits = 8 * std::mem::size_of::<u64>();

        let chunk_len = len / chunk_bits;
        let remainder_len = len & (chunk_bits - 1);

        BitChunks::<'a> {
            buffer: &buffer[byte_offset..],
            bit_offset,
            chunk_len,
            remainder_len,
        }
    }
}

#[derive(Debug)]
pub struct BitChunkIterator<'a> {
    buffer: &'a [u8],
    bit_offset: usize,
    chunk_len: usize,
    index: usize,
}

impl<'a> BitChunks<'a> {
    /// Returns the number of remaining bits, guaranteed to be between 0 and 63 (inclusive)
    #[inline]
    pub const fn remainder_len(&self) -> usize {
        self.remainder_len
    }

    /// Returns the number of chunks
    #[inline]
    pub const fn chunk_len(&self) -> usize {
        self.chunk_len
    }

    /// Returns the bitmask of remaining bits
    #[inline]
    pub fn remainder_bits(&self) -> u64 {
        let bit_len = self.remainder_len;
        if bit_len == 0 {
            0
        } else {
            let bit_offset = self.bit_offset;
            // number of bytes to read
            // might be one more than sizeof(u64) if the offset is in the middle of a byte
            let byte_len = bytes_for(bit_len + bit_offset);
            // pointer to remainder bytes after all complete chunks
            let base = unsafe {
                self.buffer
                    .as_ptr()
                    .add(self.chunk_len * std::mem::size_of::<u64>())
            };

            let mut bits = unsafe { std::ptr::read(base) } as u64 >> bit_offset;
            for i in 1..byte_len {
                let byte = unsafe { std::ptr::read(base.add(i)) };
                bits |= (byte as u64) << (i * 8 - bit_offset);
            }

            bits & ((1 << bit_len) - 1)
        }
    }

    /// Returns an iterator over chunks of 64 bits represented as an u64
    #[inline]
    pub const fn iter(&self) -> BitChunkIterator<'a> {
        BitChunkIterator::<'a> {
            buffer: self.buffer,
            bit_offset: self.bit_offset,
            chunk_len: self.chunk_len,
            index: 0,
        }
    }
}

impl<'a> IntoIterator for BitChunks<'a> {
    type Item = u64;
    type IntoIter = BitChunkIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Iterator for BitChunkIterator<'_> {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<u64> {
        let index = self.index;
        if index >= self.chunk_len {
            return None;
        }

        // cast to *const u64 should be fine since we are using read_unaligned below
        #[allow(clippy::cast_ptr_alignment)]
        let raw_data = self.buffer.as_ptr() as *const u64;

        // bit-packed buffers are stored starting with the least-significant byte first
        // so when reading as u64 on a big-endian machine, the bytes need to be swapped
        let current = unsafe { std::ptr::read_unaligned(raw_data.add(index)).to_le() };

        let combined = if self.bit_offset == 0 {
            current
        } else {
            let next = unsafe { std::ptr::read_unaligned(raw_data.add(index + 1)).to_le() };

            current >> self.bit_offset
                | (next & ((1 << self.bit_offset) - 1)) << (64 - self.bit_offset)
        };

        self.index = index + 1;

        Some(combined)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.chunk_len - self.index,
            Some(self.chunk_len - self.index),
        )
    }
}

impl ExactSizeIterator for BitChunkIterator<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.chunk_len - self.index
    }
}

#[cfg(test)]
mod tests {
    use super::BitChunks;

    #[test]
    fn test_iter_aligned() {
        let input: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7];
        let bitchunks = BitChunks::new(&input, 0, 64);

        let result = bitchunks.into_iter().collect::<Vec<_>>();

        assert_eq!(vec![0x0706050403020100], result);
    }

    #[test]
    fn test_iter_unaligned() {
        let input: &[u8] = &[
            0b00000000, 0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000, 0b00100000,
            0b01000000, 0b11111111,
        ];
        let bitchunks = BitChunks::new(&input, 4, 64);

        assert_eq!(0, bitchunks.remainder_len());
        assert_eq!(0, bitchunks.remainder_bits());

        let result = bitchunks.into_iter().collect::<Vec<_>>();

        assert_eq!(
            vec![0b1111010000000010000000010000000010000000010000000010000000010000],
            result
        );
    }

    #[test]
    fn test_iter_unaligned_remainder_1_byte() {
        let input: &[u8] = &[
            0b00000000, 0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000, 0b00100000,
            0b01000000, 0b11111111,
        ];
        let bitchunks = BitChunks::new(&input, 4, 66);

        assert_eq!(2, bitchunks.remainder_len());
        assert_eq!(0b00000011, bitchunks.remainder_bits());

        let result = bitchunks.into_iter().collect::<Vec<_>>();

        assert_eq!(
            vec![0b1111010000000010000000010000000010000000010000000010000000010000],
            result
        );
    }

    #[test]
    fn test_iter_unaligned_remainder_bits_across_bytes() {
        let input: &[u8] = &[0b00111111, 0b11111100];

        // remainder contains bits from both bytes
        // result should be the highest 2 bits from first byte followed by lowest 5 bits of second bytes
        let bitchunks = BitChunks::new(&input, 6, 7);

        assert_eq!(7, bitchunks.remainder_len());
        assert_eq!(0b1110000, bitchunks.remainder_bits());
    }

    #[test]
    fn test_iter_unaligned_remainder_bits_large() {
        let input: &[u8] = &[
            0b11111111, 0b00000000, 0b11111111, 0b00000000, 0b11111111, 0b00000000, 0b11111111,
            0b00000000, 0b11111111,
        ];
        let bitchunks = BitChunks::new(&input, 2, 63);

        assert_eq!(63, bitchunks.remainder_len());
        assert_eq!(
            0b100_0000_0011_1111_1100_0000_0011_1111_1100_0000_0011_1111_1100_0000_0011_1111,
            bitchunks.remainder_bits()
        );
    }
}
