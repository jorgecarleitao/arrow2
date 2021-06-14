mod chunk_iterator;
mod iterator;
mod slice_iterator;
mod zip_validity;

pub use chunk_iterator::{BitChunk, BitChunkIterExact, BitChunks, BitChunksExact};
pub use iterator::BitmapIter;
pub use slice_iterator::SlicesIterator;
pub use zip_validity::{zip_validity, ZipValidity};

const BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
const UNSET_BIT_MASK: [u8; 8] = [
    255 - 1,
    255 - 2,
    255 - 4,
    255 - 8,
    255 - 16,
    255 - 32,
    255 - 64,
    255 - 128,
];

/// Returns whether bit at position `i` in `byte` is set or not
#[inline]
pub fn is_set(byte: u8, i: usize) -> bool {
    (byte & BIT_MASK[i]) != 0
}

/// Sets bit at position `i` in `byte`
#[inline]
pub fn set(byte: u8, i: usize, value: bool) -> u8 {
    if value {
        byte | BIT_MASK[i]
    } else {
        byte & UNSET_BIT_MASK[i]
    }
}

/// Returns whether bit at position `i` in `data` is set or not
#[inline]
pub fn set_bit(data: &mut [u8], i: usize, value: bool) {
    data[i / 8] = set(data[i / 8], i % 8, value);
}

/// Returns whether bit at position `i` in `data` is set or not
#[inline]
pub fn get_bit(data: &[u8], i: usize) -> bool {
    is_set(data[i / 8], i % 8)
}

/// Returns whether bit at position `i` in `data` is set or not.
///
/// # Safety
/// `i >= data.len() * 8` results in undefined behavior
#[inline]
pub unsafe fn get_bit_unchecked(data: &[u8], i: usize) -> bool {
    (*data.as_ptr().add(i >> 3) & BIT_MASK[i & 7]) != 0
}

/// Returns the number of bytes required to hold `bits` bits.
#[inline]
pub fn bytes_for(bits: usize) -> usize {
    bits.saturating_add(7) / 8
}

#[inline]
pub fn null_count(slice: &[u8], offset: usize, len: usize) -> usize {
    // u64 results in optimal performance (verified via benches)
    let mut chunks = chunk_iterator::BitChunks::<u64>::new(slice, offset, len);

    let mut count: usize = chunks.by_ref().map(|c| c.count_ones() as usize).sum();

    if chunks.remainder_len() > 0 {
        // mask least significant bits up to len, as they are otherwise not required
        // here we shift instead because it is a bit faster
        let remainder = chunks.remainder() & !0u64 >> (64 - chunks.remainder_len());
        count += remainder.count_ones() as usize;
    }

    len - count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_bit() {
        let input: &[u8] = &[
            0b00000000, 0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000, 0b00100000,
            0b01000000, 0b11111111,
        ];
        for i in 0..8 {
            assert_eq!(get_bit(input, i), false);
        }
        assert_eq!(get_bit(input, 8), true);
        for i in 8 + 1..2 * 8 {
            assert_eq!(get_bit(input, i), false);
        }
        assert_eq!(get_bit(input, 2 * 8 + 1), true);
        for i in 2 * 8 + 2..3 * 8 {
            assert_eq!(get_bit(input, i), false);
        }
        assert_eq!(get_bit(input, 3 * 8 + 2), true);
        for i in 3 * 8 + 3..4 * 8 {
            assert_eq!(get_bit(input, i), false);
        }
        assert_eq!(get_bit(input, 4 * 8 + 3), true);
    }

    #[test]
    fn test_null_count() {
        let input: &[u8] = &[
            0b01001001, 0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000, 0b00100000,
            0b01000000, 0b11111111,
        ];
        assert_eq!(null_count(input, 0, 8), 8 - 3);
        assert_eq!(null_count(input, 1, 7), 7 - 2);
        assert_eq!(null_count(input, 1, 8), 8 - 3);
        assert_eq!(null_count(input, 2, 7), 7 - 3);
        assert_eq!(null_count(input, 0, 32), 32 - 6);
        assert_eq!(null_count(input, 9, 2), 2);

        let input: &[u8] = &[0b01000000, 0b01000001];
        assert_eq!(null_count(input, 8, 2), 1);
        assert_eq!(null_count(input, 8, 3), 2);
        assert_eq!(null_count(input, 8, 4), 3);
        assert_eq!(null_count(input, 8, 5), 4);
        assert_eq!(null_count(input, 8, 6), 5);
        assert_eq!(null_count(input, 8, 7), 5);
        assert_eq!(null_count(input, 8, 8), 6);

        let input: &[u8] = &[0b01000000, 0b01010101];
        assert_eq!(null_count(input, 9, 2), 1);
        assert_eq!(null_count(input, 10, 2), 1);
        assert_eq!(null_count(input, 11, 2), 1);
        assert_eq!(null_count(input, 12, 2), 1);
        assert_eq!(null_count(input, 13, 2), 1);
        assert_eq!(null_count(input, 14, 2), 1);
    }
}
