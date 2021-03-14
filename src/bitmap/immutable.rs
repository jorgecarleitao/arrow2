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
    buffer::MutableBuffer,
};

use super::{BitmapIter, MutableBitmap};

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

impl<P: AsRef<[u8]>> From<(P, usize)> for Bitmap {
    fn from((bytes, length): (P, usize)) -> Self {
        let buffer = MutableBuffer::<u8>::from(bytes.as_ref());
        (buffer, length).into()
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
