// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::ops::{BitAnd, BitOr, Not};

use crate::bits::get_bit_unchecked;

use super::{Bitmap, MutableBuffer};

/// Apply a bitwise operation `op` to two inputs and return the result as a [`Bitmap`].
pub fn binary<F>(lhs: &Bitmap, rhs: &Bitmap, op: F) -> Bitmap
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
    let mut buffer = unsafe { MutableBuffer::from_chunk_iter(chunks) };

    let remainder_bytes = lhs_chunks.remainder_len().saturating_add(7) / 8;
    let rem = op(lhs_chunks.remainder(), rhs_chunks.remainder());
    // See https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps
    // least-significant bit (LSB) numbering (also known as bit-endianness)
    let rem = &rem.to_le_bytes()[0..remainder_bytes];
    buffer.extend_from_slice(rem);

    let length = lhs.len();

    (buffer, length).into()
}

/// Apply a bitwise operation `op` to one input and return the result as a [`Bitmap`].
pub fn unary<F>(lhs: &Bitmap, op: F) -> Bitmap
where
    F: Fn(u64) -> u64,
{
    let lhs_chunks = lhs.chunks();

    let chunks = lhs_chunks.iter().map(|left| op(left));
    let mut buffer = unsafe { MutableBuffer::from_chunk_iter(chunks) };

    let remainder_bytes = lhs_chunks.remainder_len().saturating_add(7) / 8;
    let rem = op(lhs_chunks.remainder());
    // See https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps
    // least-significant bit (LSB) numbering (also known as bit-endianness)
    let rem = &rem.to_le_bytes()[0..remainder_bytes];
    buffer.extend_from_slice(rem);

    (buffer, lhs.len()).into()
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

    let lhs_chunks = lhs.chunks::<u64>();
    let rhs_chunks = rhs.chunks::<u64>();

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
    let lhs_remainder = &lhs_chunks.remainder().to_le_bytes()[0..remainder_bytes];
    let rhs_remainder = &rhs_chunks.remainder().to_le_bytes()[0..remainder_bytes];
    unsafe {
        debug_assert!(lhs_remainder.len() * 8 >= lhs_chunks.remainder_len());
        (0..lhs_chunks.remainder_len())
            .all(|i| get_bit_unchecked(lhs_remainder, i) == get_bit_unchecked(rhs_remainder, i))
    }
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
