mod chunk_iterator;
mod fmt;
mod iterator;
mod slice_iterator;
mod zip_validity;

pub use chunk_iterator::{BitChunk, BitChunkIterExact, BitChunks, BitChunksExact};
pub use fmt::fmt;
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
    //return BitmapIter::new(slice, offset, len).filter(|x| !*x).count();

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
