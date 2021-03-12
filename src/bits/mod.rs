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

mod chunk_iterator;
mod slice_iterator;
mod zip_validity;

pub use slice_iterator::SlicesIterator;
pub use zip_validity::{zip_validity, ZipValidity};

const BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];

/// Returns whether bit at position `i` in `data` is set or not
#[inline]
pub fn get_bit(data: &[u8], i: usize) -> bool {
    (data[i >> 3] & BIT_MASK[i & 7]) != 0
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
pub(crate) fn null_count(slice: &[u8], offset: usize, len: usize) -> usize {
    let chunks = chunk_iterator::BitChunks::<u64>::new(slice, offset, len);

    let mut count: usize = chunks.iter().map(|c| c.count_ones() as usize).sum();

    if chunks.remainder_len() > 0 {
        // mask least significant bits up to len, as they are otherwise not required
        // let remainder = chunks.remainder() & !0u64 >> (64 - chunks.remainder_len());
        // here we shift instead because it is a bit faster
        let remainder = chunks.remainder() & !0u64 >> (64 - chunks.remainder_len());
        count += remainder.count_ones() as usize;
    }

    len - count
}

pub use chunk_iterator::{BitChunk, BitChunkIterator, BitChunks};

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
}
