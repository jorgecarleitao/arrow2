mod chunk_iterator;

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

/// Sets bit at position `i` for `data`
#[inline]
pub fn set_bit(data: &mut [u8], i: usize) {
    data[i >> 3] |= BIT_MASK[i & 7];
}

/// Sets bit at position `i` for `data` to 0
#[inline]
pub fn unset_bit(data: &mut [u8], i: usize) {
    data[i >> 3] &= UNSET_BIT_MASK[i & 7];
}

/// Returns the number of bytes required to hold `bits` bits.
#[inline]
pub fn bytes_for(bits: usize) -> usize {
    bits.saturating_add(7) / 8
}

#[inline]
pub(crate) fn null_count(null_bit_buffer: Option<&[u8]>, offset: usize, len: usize) -> usize {
    if let Some(slice) = null_bit_buffer {
        assert!(len >= slice.len() * 8);
        let chunks = chunk_iterator::BitChunks::new(slice, offset, slice.len() * 8);

        let mut count: usize = chunks.iter().map(|c| c.count_ones() as usize).sum();
        count += chunks.remainder_bits().count_ones() as usize;

        len - count
    } else {
        0
    }
}
