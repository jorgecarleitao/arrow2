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
use std::sync::Arc;

use crate::{
    bits::{get_bit, get_bit_unchecked, null_count, BitChunk, BitChunks},
    buffer::bytes::Bytes,
};

use super::MutableBuffer;

/// An immutable container whose API is optimized to handle bitmaps. All quantities on this
/// container's API are measured in bits.
/// # Implementation
/// * This container shares memory regions across thread boundaries through an `Arc`
/// * Cloning [`Bitmap`] is `O(1)`
/// * Slicing [`Bitmap`] is `O(1)`
#[derive(Debug, Clone)]
pub struct Bitmap {
    bytes: Arc<Bytes<u8>>,
    // both are measured in bits. They are used to bound the bitmap to a region of Bytes.
    offset: usize,
    length: usize,
    // this is a cache: it must be computed on initialization
    null_count: usize,
}

impl Default for Bitmap {
    fn default() -> Self {
        MutableBitmap::new().into()
    }
}

impl Bitmap {
    /// Initializes an empty [`Bitmap`].
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn new_zeroed(length: usize) -> Self {
        MutableBitmap::from_len_zeroed(length).into()
    }

    /// Returns the length of the [`Bitmap`] in bits.
    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns whether [`Bitmap`] is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Creates a new `Bitmap` from [`Bytes`] and a length.
    /// # Panic
    /// Panics iff `length <= bytes.len() * 8`
    #[inline]
    pub(crate) fn from_bytes(bytes: Bytes<u8>, length: usize) -> Self {
        assert!(length <= bytes.len() * 8);
        let null_count = null_count(&bytes, 0, length);
        Self {
            length,
            offset: 0,
            bytes: Arc::new(bytes),
            null_count,
        }
    }

    /// Counts the nulls (unset bits) starting from `offset` bits and for `length` bits.
    #[inline]
    pub fn null_count_range(&self, offset: usize, length: usize) -> usize {
        null_count(&self.bytes, self.offset + offset, length)
    }

    /// Returns the number of unset bits on this `Bitmap`
    #[inline]
    pub fn null_count(&self) -> usize {
        self.null_count
    }

    /// Slices `self`, offseting by `offset` and truncating up to `length` bits.
    /// # Panic
    /// Panics iff `self.offset + offset + length <= self.bytes.len() * 8`, i.e. if the offset and `length`
    /// exceeds the allocated capacity of `self`.
    #[inline]
    pub fn slice(mut self, offset: usize, length: usize) -> Self {
        self.offset += offset;
        assert!(self.offset + length <= self.bytes.len() * 8);
        self.length = length;
        self.null_count = null_count(&self.bytes, self.offset, self.length);
        self
    }

    /// Returns whether the bit at position `i` is set.
    /// # Panics
    /// Panics iff `i >= self.len()`.
    #[inline]
    pub fn get_bit(&self, i: usize) -> bool {
        get_bit(&self.bytes, self.offset + i)
    }

    /// Returns whether the bit at position `i` is set.
    #[inline]
    pub fn get(&self, i: usize) -> Option<bool> {
        if i < self.len() {
            Some(unsafe { self.get_bit_unchecked(i) })
        } else {
            None
        }
    }

    /// Unsafely returns whether the bit at position `i` is set.
    /// # Safety
    /// Unsound iff `i >= self.len()`.
    #[inline]
    pub unsafe fn get_bit_unchecked(&self, i: usize) -> bool {
        get_bit_unchecked(&self.bytes, self.offset + i)
    }

    /// Returns a pointer to the start of this `Bitmap` (ignores `offsets`)
    /// This pointer is allocated iff `self.len() > 0`.
    pub(crate) fn as_ptr(&self) -> std::ptr::NonNull<u8> {
        self.bytes.ptr()
    }
}

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

impl<P: AsRef<[u8]>> From<(P, usize)> for Bitmap {
    fn from((bytes, length): (P, usize)) -> Self {
        let buffer = MutableBuffer::<u8>::from(bytes.as_ref());
        (buffer, length).into()
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

impl FromIterator<bool> for Bitmap {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = bool>,
    {
        MutableBitmap::from_iter(iter).into()
    }
}

impl Default for MutableBitmap {
    fn default() -> Self {
        Self::new()
    }
}

impl Bitmap {
    /// Returns an iterator over bits in chunks of `T`, which is useful for
    /// bit operations.
    pub fn chunks<T: BitChunk>(&self) -> BitChunks<T> {
        BitChunks::new(&self.bytes, self.offset, self.length)
    }

    #[inline]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl Bitmap {
    /// Creates a new [`Bitmap`] from an iterator of booleans.
    /// # Safety
    /// The caller must guarantee that the iterator is `TrustedLen`.
    #[inline]
    pub unsafe fn from_trusted_len_iter<I: Iterator<Item = bool>>(iterator: I) -> Self {
        MutableBitmap::from_trusted_len_iter(iterator).into()
    }

    /// Creates a new [`Bitmap`] from a fallible iterator of booleans.
    /// # Safety
    /// The caller must guarantee that the iterator is `TrustedLen`.
    #[inline]
    pub unsafe fn try_from_trusted_len_iter<E, I: Iterator<Item = std::result::Result<bool, E>>>(
        iterator: I,
    ) -> std::result::Result<Self, E> {
        Ok(MutableBitmap::try_from_trusted_len_iter(iterator)?.into())
    }
}

// Methods used for IPC
impl Bitmap {
    #[inline]
    pub(crate) fn offset(&self) -> usize {
        self.offset
    }

    #[inline]
    pub(crate) fn as_slice(&self) -> &[u8] {
        assert_eq!(self.offset % 8, 0); // slices only make sense when there is no offset
        let start = self.offset % 8;
        let len = self.length.saturating_add(7) / 8;
        &self.bytes[start..len]
    }
}

impl<'a> IntoIterator for &'a Bitmap {
    type Item = bool;
    type IntoIter = BitmapIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        BitmapIter::<'a>::new(self)
    }
}

impl<'a> Bitmap {
    /// constructs a new iterator
    pub fn iter(&'a self) -> BitmapIter<'a> {
        BitmapIter::<'a>::new(&self)
    }
}

/// Iterator of Option<T> from an iterator and validity.
pub struct BitmapIter<'a> {
    iter: std::slice::Iter<'a, u8>,
    current_byte: &'a u8,
    len: usize,
    index: usize,
    mask: u8,
}

impl<'a> BitmapIter<'a> {
    #[inline]
    pub fn new(bitmap: &'a Bitmap) -> Self {
        let offset = bitmap.offset();
        let len = bitmap.len();
        let bytes = &bitmap.bytes()[offset / 8..];

        let mut iter = bytes.iter();

        let current_byte = iter.next().unwrap_or(&0);

        Self {
            iter,
            mask: 1u8.rotate_left(offset as u32),
            len,
            index: 0,
            current_byte,
        }
    }
}

impl<'a> Iterator for BitmapIter<'a> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        // easily predictable in branching
        if self.index == self.len {
            return None;
        } else {
            self.index += 1;
        }
        let value = self.current_byte & self.mask != 0;
        self.mask = self.mask.rotate_left(1);
        if self.mask == 1 {
            // reached a new byte => try to fetch it from the iterator
            match self.iter.next() {
                Some(v) => self.current_byte = v,
                None => return None,
            }
        }
        Some(value)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len - self.index, Some(self.len - self.index))
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
