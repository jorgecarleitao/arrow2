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
    bits::{get_bit, get_bit_unchecked, null_count, set_bit_raw, unset_bit_raw, BitChunks},
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

impl Bitmap {
    /// Initializes an empty [`Bitmap`].
    #[inline]
    pub fn new() -> Self {
        MutableBitmap::new().into()
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
    length: usize,
}

impl MutableBitmap {
    /// Initializes an empty [`MutableBitmap`].
    #[inline]
    pub fn new() -> Self {
        Self {
            buffer: MutableBuffer::new(),
            length: 0,
        }
    }

    /// Initializes an a pre-allocated [`MutableBitmap`] with capacity for `capacity` bits.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: MutableBuffer::from_len_zeroed(capacity.saturating_add(7) / 8),
            length: 0,
        }
    }

    /// Pushes a new bit to the container, re-sizing it if necessary.
    #[inline]
    pub fn push(&mut self, value: bool) {
        self.buffer
            .resize((self.length + 1).saturating_add(7) / 8, 0);
        if value {
            unsafe { set_bit_raw(self.buffer.as_mut_ptr(), self.length) };
        } else {
            unsafe { unset_bit_raw(self.buffer.as_mut_ptr(), self.length) };
        }
        self.length += 1;
    }

    /// Pushes a new bit to the container.
    /// # Safety
    /// The caller must ensure that this container has the necessary capacity.
    #[inline]
    pub unsafe fn push_unchecked(&mut self, value: bool) {
        if value {
            set_bit_raw(self.buffer.as_mut_ptr(), self.length);
        } else {
            unset_bit_raw(self.buffer.as_mut_ptr(), self.length);
        }
        self.length += 1;
        self.buffer.set_len(self.length.saturating_add(7) / 8);
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
    fn from(buffer: MutableBitmap) -> Self {
        Bitmap::from_bytes(buffer.buffer.into(), buffer.length)
    }
}

impl From<MutableBitmap> for Option<Bitmap> {
    #[inline]
    fn from(buffer: MutableBitmap) -> Self {
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
        Self { buffer, length }
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

impl Bitmap {
    /// Returns an iterator over bits in chunks of `u64`, which is useful for
    /// bit operations.
    pub fn chunks(&self) -> BitChunks {
        BitChunks::new(&self.bytes, self.offset, self.length)
    }
}

impl Bitmap {
    /// Creates a new [`Bitmap`] from an iterator of booleans.
    /// # Safety
    /// The caller must guarantee that the iterator is `TrustedLen`.
    #[inline]
    pub unsafe fn from_trusted_len_iter<I: Iterator<Item = bool>>(iterator: I) -> Self {
        // todo implement `from_trusted_len_iter` for MutableBitmap
        MutableBitmap::from_iter(iterator).into()
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

/// an iterator that returns Some(bool) or None.
// Note: This implementation is based on std's [Vec]s' [IntoIter].
#[derive(Debug)]
pub struct BitmapIter<'a> {
    bitmap: &'a Bitmap,
    current: usize,
}

impl<'a> BitmapIter<'a> {
    /// create a new iterator
    #[inline]
    pub fn new(bitmap: &'a Bitmap) -> Self {
        BitmapIter { bitmap, current: 0 }
    }
}

impl<'a> std::iter::Iterator for BitmapIter<'a> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.bitmap.len() {
            None
        } else {
            let old = self.current;
            self.current += 1;
            Some(unsafe { self.bitmap.get_bit_unchecked(old) })
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.bitmap.len() - self.current,
            Some(self.bitmap.len() - self.current),
        )
    }
}
