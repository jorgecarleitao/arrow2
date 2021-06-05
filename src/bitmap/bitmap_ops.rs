use std::ops::{BitAnd, BitOr, Not};

use crate::buffer::MutableBuffer;

use super::Bitmap;

/// Apply a bitwise operation `op` to four inputs and return the result as a [`Bitmap`].
pub fn quaternary<F>(a1: &Bitmap, a2: &Bitmap, a3: &Bitmap, a4: &Bitmap, op: F) -> Bitmap
where
    F: Fn(u64, u64, u64, u64) -> u64,
{
    assert_eq!(a1.len(), a2.len());
    assert_eq!(a1.len(), a3.len());
    assert_eq!(a1.len(), a4.len());
    let mut a1_chunks = a1.chunks();
    let mut a2_chunks = a2.chunks();
    let mut a3_chunks = a3.chunks();
    let mut a4_chunks = a4.chunks();

    let chunks = a1_chunks
        .by_ref()
        .zip(a2_chunks.by_ref())
        .zip(a3_chunks.by_ref())
        .zip(a4_chunks.by_ref())
        .map(|(((a1, a2), a3), a4)| op(a1, a2, a3, a4));
    // Soundness: `BitChunks` is a trusted len iterator
    let mut buffer = unsafe { MutableBuffer::from_chunk_iter_unchecked(chunks) };

    let remainder_bytes = a1_chunks.remainder_len().saturating_add(7) / 8;
    let rem = op(
        a1_chunks.remainder(),
        a2_chunks.remainder(),
        a3_chunks.remainder(),
        a4_chunks.remainder(),
    );
    let rem = &rem.to_ne_bytes()[..remainder_bytes];
    buffer.extend_from_slice(rem);

    let length = a1.len();

    Bitmap::from_u8_buffer(buffer, length)
}

/// Apply a bitwise operation `op` to three inputs and return the result as a [`Bitmap`].
pub fn ternary<F>(a1: &Bitmap, a2: &Bitmap, a3: &Bitmap, op: F) -> Bitmap
where
    F: Fn(u64, u64, u64) -> u64,
{
    assert_eq!(a1.len(), a2.len());
    assert_eq!(a1.len(), a3.len());
    let mut a1_chunks = a1.chunks();
    let mut a2_chunks = a2.chunks();
    let mut a3_chunks = a3.chunks();

    let chunks = a1_chunks
        .by_ref()
        .zip(a2_chunks.by_ref())
        .zip(a3_chunks.by_ref())
        .map(|((a1, a2), a3)| op(a1, a2, a3));
    // Soundness: `BitChunks` is a trusted len iterator
    let mut buffer = unsafe { MutableBuffer::from_chunk_iter_unchecked(chunks) };

    let remainder_bytes = a1_chunks.remainder_len().saturating_add(7) / 8;
    let rem = op(
        a1_chunks.remainder(),
        a2_chunks.remainder(),
        a3_chunks.remainder(),
    );
    let rem = &rem.to_ne_bytes()[..remainder_bytes];
    buffer.extend_from_slice(rem);

    let length = a1.len();

    Bitmap::from_u8_buffer(buffer, length)
}

/// Apply a bitwise operation `op` to two inputs and return the result as a [`Bitmap`].
pub fn binary<F>(lhs: &Bitmap, rhs: &Bitmap, op: F) -> Bitmap
where
    F: Fn(u64, u64) -> u64,
{
    assert_eq!(lhs.len(), rhs.len());
    let mut lhs_chunks = lhs.chunks();
    let mut rhs_chunks = rhs.chunks();

    let chunks = lhs_chunks
        .by_ref()
        .zip(rhs_chunks.by_ref())
        .map(|(left, right)| op(left, right));
    // Soundness: `BitChunks` is a trusted len iterator
    let mut buffer = unsafe { MutableBuffer::from_chunk_iter_unchecked(chunks) };

    let remainder_bytes = lhs_chunks.remainder_len().saturating_add(7) / 8;
    let rem = op(lhs_chunks.remainder(), rhs_chunks.remainder());
    let rem = &rem.to_ne_bytes()[..remainder_bytes];
    buffer.extend_from_slice(rem);

    let length = lhs.len();

    Bitmap::from_u8_buffer(buffer, length)
}

/// Apply a bitwise operation `op` to one input and return the result as a [`Bitmap`].
pub fn unary<F>(lhs: &Bitmap, op: F) -> Bitmap
where
    F: Fn(u64) -> u64,
{
    let mut lhs_chunks = lhs.chunks();

    let chunks = lhs_chunks.by_ref().map(|left| op(left));
    let mut buffer = unsafe { MutableBuffer::from_chunk_iter_unchecked(chunks) };

    let remainder_bytes = lhs_chunks.remainder_len().saturating_add(7) / 8;
    let rem = op(lhs_chunks.remainder());

    let rem = &rem.to_ne_bytes()[..remainder_bytes];
    buffer.extend_from_slice(rem);

    Bitmap::from_u8_buffer(buffer, lhs.len())
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

    let mut lhs_chunks = lhs.chunks::<u64>();
    let mut rhs_chunks = rhs.chunks::<u64>();

    let equal_chunks = lhs_chunks
        .by_ref()
        .zip(rhs_chunks.by_ref())
        .all(|(left, right)| left == right);

    if !equal_chunks {
        return false;
    }
    let lhs_remainder = lhs_chunks.remainder_iter();
    let rhs_remainder = rhs_chunks.remainder_iter();
    lhs_remainder.zip(rhs_remainder).all(|(x, y)| x == y)
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

    use crate::bitmap::Bitmap;
    use crate::buffer::MutableBuffer;

    fn create_bitmap<P: AsRef<[u8]>>(bytes: P, len: usize) -> Bitmap {
        let buffer = MutableBuffer::<u8>::from(bytes.as_ref());
        Bitmap::from_u8_buffer(buffer, len)
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
    fn test_or_large() {
        let input: &[u8] = &[
            0b00000000, 0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000, 0b00100000,
            0b01000010, 0b11111111,
        ];
        let input1: &[u8] = &[
            0b00000000, 0b00000001, 0b10000000, 0b10000000, 0b10000000, 0b10000000, 0b10000000,
            0b10000000, 0b11111111,
        ];
        let expected: &[u8] = &[
            0b00000000, 0b00000001, 0b10000010, 0b10000100, 0b10001000, 0b10010000, 0b10100000,
            0b11000010, 0b11111111,
        ];

        let lhs = create_bitmap(input, 62);
        let rhs = create_bitmap(input1, 62);
        let expected = create_bitmap(expected, 62);
        assert_eq!(&lhs | &rhs, expected);
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
