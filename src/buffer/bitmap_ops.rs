use std::ops::{BitAnd, BitOr, Not};

use crate::bits::get_bit_unchecked;

use super::{Bitmap, MutableBuffer};

/// Apply a bitwise operation `op` to two inputs and return the result as a [`Bitmap`].
fn binary<F>(lhs: &Bitmap, rhs: &Bitmap, op: F) -> Bitmap
where
    F: Fn(u64, u64) -> u64,
{
    assert_eq!(lhs.len(), rhs.len());
    let lhs_chunks = lhs.chunks();
    let rhs_chunks = rhs.chunks();

    let chunks = lhs_chunks
        .iter()
        .zip(rhs_chunks.iter())
        .map(|(left, right)| op(left, right));
    // Soundness: `BitChunks` is a trusted len iterator
    let mut buffer = MutableBuffer::from_chunk_iter(chunks);

    let remainder_bytes = lhs_chunks.remainder_len().saturating_add(7) / 8;
    let rem = op(lhs_chunks.remainder_bits(), rhs_chunks.remainder_bits());
    // See https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps
    // least-significant bit (LSB) numbering (also known as bit-endianness)
    let rem = &rem.to_le_bytes()[0..remainder_bytes];
    buffer.extend_from_slice(rem);

    let length = lhs_chunks.remainder_len() + lhs_chunks.chunk_len() * 64;

    (buffer, length).into()
}

/// Apply a bitwise operation `op` to one input and return the result as a [`Bitmap`].
fn unary<F>(lhs: &Bitmap, op: F) -> Bitmap
where
    F: Fn(u64) -> u64,
{
    let lhs_chunks = lhs.chunks();

    let chunks = lhs_chunks.iter().map(|left| op(left));
    let mut buffer = MutableBuffer::from_chunk_iter(chunks);

    let remainder_bytes = lhs_chunks.remainder_len().saturating_add(7) / 8;
    let rem = op(lhs_chunks.remainder_bits());
    // See https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps
    // least-significant bit (LSB) numbering (also known as bit-endianness)
    let rem = &rem.to_le_bytes()[0..remainder_bytes];
    buffer.extend_from_slice(rem);

    let length = lhs_chunks.remainder_len() + lhs_chunks.chunk_len() * 64;

    (buffer, length).into()
}

fn and(lhs: &Bitmap, rhs: &Bitmap) -> Bitmap {
    binary(lhs, rhs, |x, y| x & y)
}

fn or(lhs: &Bitmap, rhs: &Bitmap) -> Bitmap {
    binary(lhs, rhs, |x, y| x | y)
}

fn eq(lhs: &Bitmap, rhs: &Bitmap) -> bool {
    if lhs.len() != rhs.len() {
        return false;
    }

    let lhs_chunks = lhs.chunks();
    let rhs_chunks = rhs.chunks();

    let equal_chunks = lhs_chunks
        .iter()
        .zip(rhs_chunks.iter())
        .all(|(left, right)| left == right);

    if !equal_chunks {
        return false;
    }
    let remainder_bytes = lhs_chunks.remainder_len().saturating_add(7) / 8;

    // See https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps
    // least-significant bit (LSB) numbering (also known as bit-endianness)
    let lhs_remainder = &lhs_chunks.remainder_bits().to_le_bytes()[0..remainder_bytes];
    let rhs_remainder = &rhs_chunks.remainder_bits().to_le_bytes()[0..remainder_bytes];
    let remainder_equal = unsafe {
        debug_assert!(lhs_remainder.len() * 8 >= lhs_chunks.remainder_len());
        (0..lhs_chunks.remainder_len())
            .all(|i| get_bit_unchecked(lhs_remainder, i) == get_bit_unchecked(rhs_remainder, i))
    };
    remainder_equal
}

impl PartialEq for Bitmap {
    fn eq(&self, other: &Self) -> bool {
        eq(self, other)
    }
}

impl<'a, 'b> BitOr<&'b Bitmap> for &'a Bitmap {
    type Output = Bitmap;

    fn bitor(self, rhs: &'b Bitmap) -> Bitmap {
        or(self, rhs)
    }
}

impl<'a, 'b> BitAnd<&'b Bitmap> for &'a Bitmap {
    type Output = Bitmap;

    fn bitand(self, rhs: &'b Bitmap) -> Bitmap {
        and(self, rhs)
    }
}

impl Not for &Bitmap {
    type Output = Bitmap;

    fn not(self) -> Bitmap {
        unary(&self, |a| !a)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::buffer::{Bitmap, MutableBuffer};

    fn create_bitmap<P: AsRef<[u8]>>(bytes: P, len: usize) -> Bitmap {
        let buffer = MutableBuffer::<u8>::from(bytes.as_ref());
        (buffer, len).into()
    }

    #[test]
    fn test_eq() {
        let lhs = create_bitmap([0b01101010], 8);
        let rhs = create_bitmap([0b01001110], 8);
        assert_eq!(eq(&lhs, &rhs), false);
        assert_eq!(eq(&lhs, &lhs), true);
    }

    #[test]
    fn test_eq_len() {
        let lhs = create_bitmap([0b01101010], 6);
        let rhs = create_bitmap([0b00101010], 6);
        assert_eq!(eq(&lhs, &rhs), true);
        let rhs = create_bitmap([0b00001010], 6);
        assert_eq!(eq(&lhs, &rhs), false);
    }

    #[test]
    fn test_eq_slice() {
        let lhs = create_bitmap([0b10101010], 8).slice(1, 7);
        let rhs = create_bitmap([0b10101011], 8).slice(1, 7);
        assert_eq!(eq(&lhs, &rhs), true);

        let lhs = create_bitmap([0b10101010], 8).slice(2, 6);
        let rhs = create_bitmap([0b10101110], 8).slice(2, 6);
        assert_eq!(eq(&lhs, &rhs), false);
    }

    #[test]
    fn test_and() {
        let lhs = create_bitmap([0b01101010], 8);
        let rhs = create_bitmap([0b01001110], 8);
        let expected = create_bitmap([0b01001010], 8);
        assert_eq!(&lhs & &rhs, expected);
    }

    #[test]
    fn test_and_offset() {
        let lhs = create_bitmap([0b01101011], 8).slice(1, 7);
        let rhs = create_bitmap([0b01001111], 8).slice(1, 7);
        let expected = create_bitmap([0b01001010], 8).slice(1, 7);
        assert_eq!(&lhs & &rhs, expected);
    }

    #[test]
    fn test_or() {
        let lhs = create_bitmap([0b01101010], 8);
        let rhs = create_bitmap([0b01001110], 8);
        let expected = create_bitmap([0b01101110], 8);
        assert_eq!(&lhs | &rhs, expected);
    }

    #[test]
    fn test_not() {
        let lhs = create_bitmap([0b01101010], 6);
        let expected = create_bitmap([0b00010101], 6);
        assert_eq!(!&lhs, expected);
    }
}
