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

use std::iter::FromIterator;

use crate::{bits::null_count, buffer::MutableBuffer};

use super::Bitmap;

/// The mutable counterpart of [`Bitmap`].
#[derive(Debug)]
pub struct MutableBitmap {
    buffer: MutableBuffer<u8>,
    mask: u8,
    byte: u8,
    length: usize,
}

impl MutableBitmap {
    /// Initializes an empty [`MutableBitmap`].
    #[inline]
    pub fn new() -> Self {
        Self {
            buffer: MutableBuffer::new(),
            mask: 1,
            byte: 0,
            length: 0,
        }
    }

    /// Initializes a zeroed [`MutableBitmap`].
    #[inline]
    pub fn from_len_zeroed(length: usize) -> Self {
        Self {
            buffer: MutableBuffer::from_len_zeroed(length.saturating_add(7) / 8),
            mask: 1,
            byte: 0,
            length,
        }
    }

    /// Initializes an a pre-allocated [`MutableBitmap`] with capacity for `capacity` bits.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: MutableBuffer::with_capacity(capacity.saturating_add(7) / 8),
            mask: 1,
            byte: 0,
            length: 0,
        }
    }

    /// Initializes an a pre-allocated [`MutableBitmap`] with capacity for `capacity` bits.
    #[inline(always)]
    pub fn reserve(&mut self, additional: usize) {
        self.buffer
            .reserve((self.length + additional).saturating_add(7) / 8 - self.buffer.len())
    }

    /// Pushes a new bit to the container, re-sizing it if necessary.
    #[inline]
    pub fn push(&mut self, value: bool) {
        if value {
            self.byte |= self.mask
        };
        self.mask = self.mask.rotate_left(1);
        if self.mask == 1 {
            self.buffer.push(self.byte);
            self.byte = 0;
        }
        self.length += 1;
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.buffer.capacity() * 8
    }

    /// Pushes a new bit to the container
    /// # Safety
    /// The caller must ensure that the container has sufficient space.
    #[inline]
    pub unsafe fn push_unchecked(&mut self, value: bool) {
        if value {
            self.byte |= self.mask
        };
        self.mask = self.mask.rotate_left(1);
        if self.mask == 1 {
            self.buffer.push_unchecked(self.byte);
            self.byte = 0;
        }
        self.length += 1;
    }

    /// Returns the number of unset bits on this `Bitmap`
    #[inline]
    pub fn null_count(&self) -> usize {
        null_count(&self.buffer, 0, self.length)
    }

    /// Returns the length of the [`MutableBitmap`] in bits.
    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns whether [`MutableBitmap`] is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// # Safety
    /// The caller must ensure that the [`MutableBitmap`] was properly initialized up to `len`.
    #[inline]
    pub(crate) unsafe fn set_len(&mut self, len: usize) {
        self.buffer.set_len(len.saturating_add(7) / 8);
        self.length = len;
        self.mask = 1u8.rotate_left(len as u32);
    }
}

impl From<(MutableBuffer<u8>, usize)> for Bitmap {
    #[inline]
    fn from((buffer, length): (MutableBuffer<u8>, usize)) -> Self {
        Bitmap::from_bytes(buffer.into(), length)
    }
}

impl From<MutableBitmap> for Bitmap {
    #[inline]
    fn from(mut buffer: MutableBitmap) -> Self {
        if buffer.mask != 1 {
            buffer.buffer.push(buffer.byte)
        };
        Bitmap::from_bytes(buffer.buffer.into(), buffer.length)
    }
}

impl From<MutableBitmap> for Option<Bitmap> {
    #[inline]
    fn from(mut buffer: MutableBitmap) -> Self {
        if buffer.mask != 1 {
            buffer.buffer.push(buffer.byte)
        };
        if buffer.null_count() > 0 {
            Some(Bitmap::from_bytes(buffer.buffer.into(), buffer.length))
        } else {
            None
        }
    }
}

impl FromIterator<bool> for MutableBitmap {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = bool>,
    {
        let mut iterator = iter.into_iter();
        let mut buffer = {
            let byte_capacity: usize = iterator.size_hint().0.saturating_add(7) / 8;
            MutableBuffer::with_capacity(byte_capacity)
        };

        let mut length = 0;

        loop {
            let mut exhausted = false;
            let mut byte_accum: u8 = 0;
            let mut mask: u8 = 1;

            //collect (up to) 8 bits into a byte
            while mask != 0 {
                if let Some(value) = iterator.next() {
                    length += 1;
                    byte_accum |= match value {
                        true => mask,
                        false => 0,
                    };
                    mask <<= 1;
                } else {
                    exhausted = true;
                    break;
                }
            }

            // break if the iterator was exhausted before it provided a bool for this byte
            if exhausted && mask == 1 {
                break;
            }

            //ensure we have capacity to write the byte
            if buffer.len() == buffer.capacity() {
                //no capacity for new byte, allocate 1 byte more (plus however many more the iterator advertises)
                let additional_byte_capacity = 1usize.saturating_add(
                    iterator.size_hint().0.saturating_add(7) / 8, //convert bit count to byte count, rounding up
                );
                buffer.reserve(additional_byte_capacity)
            }

            // Soundness: capacity was allocated above
            unsafe { buffer.push_unchecked(byte_accum) };
            if exhausted {
                break;
            }
        }
        Self {
            buffer,
            byte: 0,
            length,
            mask: 1u8.rotate_left(length as u32),
        }
    }
}

impl MutableBitmap {
    /// Creates a new [`Bitmap`] from an iterator of booleans.
    /// # Safety
    /// The caller must guarantee that the iterator is `TrustedLen`.
    #[inline]
    pub unsafe fn from_trusted_len_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = bool>,
    {
        let mut iterator = iter.into_iter();
        let length = iterator.size_hint().1.unwrap();

        let chunks = length / 8;

        let mut mask: u8 = 1;
        let iter = (0..chunks).map(|_| {
            let mut byte_accum: u8 = 0;
            (0..8).for_each(|_| {
                let value = iterator.next().unwrap();
                byte_accum |= match value {
                    true => mask,
                    false => 0,
                };
                mask = mask.rotate_left(1);
            });
            byte_accum
        });

        let mut buffer = MutableBuffer::from_trusted_len_iter(iter);

        let mut byte_accum: u8 = 0;
        let mut mask: u8 = 1;
        for value in iterator {
            byte_accum |= match value {
                true => mask,
                false => 0,
            };
            mask = mask.rotate_left(1);
        }
        buffer.push(byte_accum);

        Self {
            buffer,
            byte: 0,
            mask,
            length,
        }
    }

    /// Creates a new [`Bitmap`] from an falible iterator of booleans.
    /// # Safety
    /// The caller must guarantee that the iterator is `TrustedLen`.
    #[inline]
    pub unsafe fn try_from_trusted_len_iter<E, I>(iter: I) -> std::result::Result<Self, E>
    where
        I: IntoIterator<Item = std::result::Result<bool, E>>,
    {
        let mut iterator = iter.into_iter();
        let length = iterator.size_hint().1.unwrap();

        let chunks = length / 8;

        let mut mask: u8 = 1;
        let iter = (0..chunks).map(|_| {
            let mut byte_accum: u8 = 0;
            (0..8).try_for_each(|_| {
                let value = iterator.next().unwrap()?;
                byte_accum |= match value {
                    true => mask,
                    false => 0,
                };
                mask = mask.rotate_left(1);
                Ok(())
            })?;
            Ok(byte_accum)
        });

        let mut buffer = MutableBuffer::try_from_trusted_len_iter(iter)?;

        let mut byte_accum: u8 = 0;
        let mut mask: u8 = 1;
        for value in iterator {
            byte_accum |= match value? {
                true => mask,
                false => 0,
            };
            mask = mask.rotate_left(1);
        }
        buffer.push(byte_accum);

        Ok(Self {
            buffer,
            byte: 0,
            mask,
            length,
        })
    }
}

impl Default for MutableBitmap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trusted_len() {
        let data = vec![true; 65];
        let bitmap = unsafe { MutableBitmap::from_trusted_len_iter(data) };
        let bitmap: Bitmap = bitmap.into();
        assert_eq!(bitmap.len(), 65);

        assert_eq!(bitmap.as_slice()[8], 0b00000001);
    }

    #[test]
    fn test_trusted_len_small() {
        let data = vec![true; 7];
        let bitmap = unsafe { MutableBitmap::from_trusted_len_iter(data) };
        let bitmap: Bitmap = bitmap.into();
        assert_eq!(bitmap.len(), 7);

        assert_eq!(bitmap.as_slice()[0], 0b01111111);
    }

    #[test]
    fn test_push() {
        let mut bitmap = MutableBitmap::new();
        bitmap.push(true);
        bitmap.push(false);
        bitmap.push(false);
        for _ in 0..7 {
            bitmap.push(true)
        }
        let bitmap: Bitmap = bitmap.into();
        assert_eq!(bitmap.len(), 10);

        assert_eq!(bitmap.as_slice()[0], 0b11111001);
        assert_eq!(bitmap.as_slice()[1], 0b00000011);
    }

    #[test]
    fn test_push_small() {
        let mut bitmap = MutableBitmap::new();
        bitmap.push(true);
        bitmap.push(true);
        bitmap.push(false);
        let bitmap: Option<Bitmap> = bitmap.into();
        let bitmap = bitmap.unwrap();
        assert_eq!(bitmap.len(), 3);
        assert_eq!(bitmap.as_slice()[0], 0b00000011);
    }

    #[test]
    fn test_push_exact_zeros() {
        let mut bitmap = MutableBitmap::new();
        for _ in 0..8 {
            bitmap.push(false)
        }
        let bitmap: Option<Bitmap> = bitmap.into();
        let bitmap = bitmap.unwrap();
        assert_eq!(bitmap.len(), 8);
        assert_eq!(bitmap.as_slice().len(), 1);
    }

    #[test]
    fn test_push_exact_ones() {
        let mut bitmap = MutableBitmap::new();
        for _ in 0..8 {
            bitmap.push(true)
        }
        let bitmap: Option<Bitmap> = bitmap.into();
        assert!(bitmap.is_none());
    }

    #[test]
    fn test_capacity() {
        let b = MutableBitmap::with_capacity(10);
        assert_eq!(b.capacity(), 512);

        let b = MutableBitmap::with_capacity(512);
        assert_eq!(b.capacity(), 512);

        let mut b = MutableBitmap::with_capacity(512);
        b.reserve(8);
        assert_eq!(b.capacity(), 512);
    }

    #[test]
    fn test_capacity_push() {
        let mut b = MutableBitmap::with_capacity(512);
        (0..512).for_each(|_| b.push(true));
        assert_eq!(b.capacity(), 512);
        b.reserve(8);
        assert_eq!(b.capacity(), 1024);
    }
}
