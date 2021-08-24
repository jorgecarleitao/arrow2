use std::iter::FromIterator;

use crate::bitmap::utils::merge_reversed;
use crate::{buffer::MutableBuffer, trusted_len::TrustedLen};

use super::utils::{fmt, get_bit, null_count, set, set_bit, BitmapIter};
use super::Bitmap;

/// A container to store booleans. [`MutableBitmap`] is semantically equivalent
/// to [`Vec<bool>`], but each value is stored as a single bit, thereby achieving a compression of 8x.
/// This container is the counterpart of [`MutableBuffer`] for boolean values.
/// [`MutableBitmap`] can be converted to a [`Bitmap`] at `O(1)`.
/// The main difference against [`Vec<bool>`] is that a bitmap cannot be represented as `&[bool]`.
/// # Implementation
/// This container is backed by [`MutableBuffer<u8>`].
pub struct MutableBitmap {
    buffer: MutableBuffer<u8>,
    // invariant: length.saturating_add(7) / 8 == buffer.len();
    length: usize,
}

impl std::fmt::Debug for MutableBitmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt(&self.buffer, 0, self.len(), f)
    }
}

impl PartialEq for MutableBitmap {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
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

    /// Empties the [`MutableBitmap`].
    #[inline]
    pub fn clear(&mut self) {
        self.length = 0;
        self.buffer.clear();
    }

    /// Initializes a zeroed [`MutableBitmap`].
    #[inline]
    pub fn from_len_zeroed(length: usize) -> Self {
        Self {
            buffer: MutableBuffer::from_len_zeroed(length.saturating_add(7) / 8),
            length,
        }
    }

    /// Initializes an a pre-allocated [`MutableBitmap`] with capacity for `capacity` bits.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: MutableBuffer::with_capacity(capacity.saturating_add(7) / 8),
            length: 0,
        }
    }

    /// Initializes an a pre-allocated [`MutableBitmap`] with capacity for `capacity` bits.
    #[inline(always)]
    pub fn reserve(&mut self, additional: usize) {
        self.buffer
            .reserve((self.length + additional).saturating_add(7) / 8 - self.buffer.len())
    }

    /// Pushes a new bit to the [`MutableBitmap`], re-sizing it if necessary.
    #[inline]
    pub fn push(&mut self, value: bool) {
        if self.length % 8 == 0 {
            self.buffer.push(0);
        }
        let byte = self.buffer.as_mut_slice().last_mut().unwrap();
        *byte = set(*byte, self.length % 8, value);
        self.length += 1;
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.buffer.capacity() * 8
    }

    /// Pushes a new bit to the [`MutableBitmap`]
    /// # Safety
    /// The caller must ensure that the [`MutableBitmap`] has sufficient capacity.
    #[inline]
    pub unsafe fn push_unchecked(&mut self, value: bool) {
        if self.length % 8 == 0 {
            self.buffer.push_unchecked(0);
        }
        if value {
            let byte = self.buffer.as_mut_slice().last_mut().unwrap();
            *byte = set(*byte, self.length % 8, true);
        };
        self.length += 1;
    }

    /// Returns the number of unset bits on this [`MutableBitmap`].
    #[inline]
    pub fn null_count(&self) -> usize {
        null_count(&self.buffer, 0, self.length)
    }

    /// Returns the length of the [`MutableBitmap`].
    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns whether [`MutableBitmap`] is empty.
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

    fn extend_set(&mut self, mut additional: usize) {
        let offset = self.length % 8;
        let added = if offset != 0 {
            // offset != 0 => at least one byte in the buffer
            let last_index = self.buffer.len() - 1;
            let last = &mut self.buffer[last_index];

            let remaining = 0b11111111u8;
            let remaining = remaining >> 8usize.saturating_sub(additional);
            let remaining = remaining << offset;
            *last |= remaining;
            std::cmp::min(additional, 8 - offset)
        } else {
            0
        };
        self.length += added;
        additional = additional.saturating_sub(added);
        if additional > 0 {
            debug_assert_eq!(self.length % 8, 0);
            let existing = self.length.saturating_add(7) / 8;
            let required = (self.length + additional).saturating_add(7) / 8;
            // add remaining as full bytes
            self.buffer.extend_from_trusted_len_iter(
                std::iter::repeat(0b11111111u8).take(required - existing),
            );
            self.length += additional;
        }
    }

    fn extend_unset(&mut self, mut additional: usize) {
        let offset = self.length % 8;
        let added = if offset != 0 {
            // offset != 0 => at least one byte in the buffer
            let last_index = self.buffer.len() - 1;
            let last = &mut self.buffer[last_index];
            *last &= 0b11111111u8 >> (8 - offset); // unset them
            std::cmp::min(additional, 8 - offset)
        } else {
            0
        };
        self.length += added;
        additional = additional.saturating_sub(added);
        if additional > 0 {
            debug_assert_eq!(self.length % 8, 0);
            self.buffer
                .resize((self.length + additional).saturating_add(7) / 8, 0);
            self.length += additional;
        }
    }

    /// Extends [`MutableBitmap`] by `additional` values of constant `value`.
    #[inline]
    pub fn extend_constant(&mut self, additional: usize, value: bool) {
        if additional == 0 {
            return;
        }

        if value {
            self.extend_set(additional)
        } else {
            self.extend_unset(additional)
        }
    }

    /// Returns whether the position `index` is set.
    /// # Panics
    /// Panics iff `index >= self.len()`.
    #[inline]
    pub fn get(&self, index: usize) -> bool {
        get_bit(&self.buffer, index)
    }

    /// Sets the position `index` to `value`
    /// # Panics
    /// Panics iff `index >= self.len()`.
    #[inline]
    pub fn set(&mut self, index: usize, value: bool) {
        set_bit(&mut self.buffer.as_mut_slice(), index, value)
    }
}

impl MutableBitmap {
    /// Initializes a [`MutableBitmap`] from a [`MutableBuffer<u8>`] and a length.
    /// # Panic
    /// Panics iff the length is larger than the length of the buffer times 8.
    #[inline]
    pub fn from_buffer(buffer: MutableBuffer<u8>, length: usize) -> Self {
        assert!(length <= buffer.len() * 8);
        Self { buffer, length }
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

impl<P: AsRef<[bool]>> From<P> for MutableBitmap {
    #[inline]
    fn from(slice: P) -> Self {
        MutableBitmap::from_trusted_len_iter(slice.as_ref().iter().copied())
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

#[inline]
fn extend<I: Iterator<Item = bool>>(buffer: &mut [u8], length: usize, mut iterator: I) {
    let chunks = length / 8;
    let reminder = length % 8;

    buffer[..chunks].iter_mut().for_each(|byte| {
        (0..8).for_each(|i| {
            if iterator.next().unwrap() {
                *byte = set(*byte, i, true)
            }
        })
    });

    if reminder != 0 {
        let last = &mut buffer[chunks];
        iterator.enumerate().for_each(|(i, value)| {
            if value {
                *last = set(*last, i, true)
            }
        });
    }
}

impl MutableBitmap {
    /// Extends `self` from a [`TrustedLen`] iterator.
    #[inline]
    pub fn extend_from_trusted_len_iter<I: TrustedLen<Item = bool>>(&mut self, iterator: I) {
        // safety: I: TrustedLen
        unsafe { self.extend_from_trusted_len_iter_unchecked(iterator) }
    }

    /// Extends `self` from an iterator of trusted len.
    /// # Safety
    /// The caller must guarantee that the iterator has a trusted len.
    pub unsafe fn extend_from_trusted_len_iter_unchecked<I: Iterator<Item = bool>>(
        &mut self,
        mut iterator: I,
    ) {
        // the length of the iterator throughout this function.
        let mut length = iterator.size_hint().1.unwrap();

        let bit_offset = self.length % 8;

        if length < 8 - bit_offset {
            if bit_offset == 0 {
                self.buffer.push(0);
            }
            // the iterator will not fill the last byte
            let byte = self.buffer.as_mut_slice().last_mut().unwrap();
            let mut i = bit_offset;
            for value in iterator {
                if value {
                    *byte = set(*byte, i, true);
                }
                i += 1;
            }
            self.length += length;
            return;
        }

        // at this point we know that length will hit a byte boundary and thus
        // increase the buffer.

        if bit_offset != 0 {
            // we are in the middle of a byte; lets finish it
            let byte = self.buffer.as_mut_slice().last_mut().unwrap();
            (bit_offset..8).for_each(|i| {
                let value = iterator.next().unwrap();
                if value {
                    *byte = set(*byte, i, true);
                }
            });
            self.length += 8 - bit_offset;
            length -= 8 - bit_offset;
        }

        // everything is aligned; proceed with the bulk operation
        debug_assert_eq!(self.length % 8, 0);

        self.buffer.extend_constant((length + 7) / 8, 0);

        extend(&mut self.buffer[self.length / 8..], length, iterator);
        self.length += length;
    }

    /// Creates a new [`MutableBitmap`] from an iterator of booleans.
    /// # Safety
    /// The iterator must report an accurate length.
    pub unsafe fn from_trusted_len_iter_unchecked<I>(iterator: I) -> Self
    where
        I: Iterator<Item = bool>,
    {
        let length = iterator.size_hint().1.unwrap();

        let mut buffer = MutableBuffer::<u8>::from_len_zeroed((length + 7) / 8);

        extend(&mut buffer, length, iterator);

        Self { buffer, length }
    }

    /// Creates a new [`MutableBitmap`] from an iterator of booleans.
    pub fn from_trusted_len_iter<I>(iterator: I) -> Self
    where
        I: TrustedLen<Item = bool>,
    {
        let length = iterator.size_hint().1.unwrap();

        let mut buffer = MutableBuffer::<u8>::from_len_zeroed((length + 7) / 8);

        extend(&mut buffer, length, iterator);

        Self { buffer, length }
    }

    /// Creates a new [`MutableBitmap`] from an iterator of booleans.
    pub fn try_from_trusted_len_iter<E, I>(iterator: I) -> std::result::Result<Self, E>
    where
        I: TrustedLen<Item = std::result::Result<bool, E>>,
    {
        unsafe { Self::try_from_trusted_len_iter_unchecked(iterator) }
    }

    /// Creates a new [`MutableBitmap`] from an falible iterator of booleans.
    /// # Safety
    /// The caller must guarantee that the iterator is `TrustedLen`.
    pub unsafe fn try_from_trusted_len_iter_unchecked<E, I>(
        mut iterator: I,
    ) -> std::result::Result<Self, E>
    where
        I: Iterator<Item = std::result::Result<bool, E>>,
    {
        let length = iterator.size_hint().1.unwrap();

        let mut buffer = MutableBuffer::<u8>::from_len_zeroed((length + 7) / 8);

        let chunks = length / 8;
        let reminder = length % 8;

        let data = buffer.as_mut_slice();
        data[..chunks].iter_mut().try_for_each(|byte| {
            (0..8).try_for_each(|i| {
                if iterator.next().unwrap()? {
                    *byte = set(*byte, i, true)
                };
                Ok(())
            })
        })?;

        if reminder != 0 {
            let last = &mut data[chunks];
            iterator.enumerate().try_for_each(|(i, value)| {
                if value? {
                    *last = set(*last, i, true)
                }
                Ok(())
            })?;
        }

        Ok(Self { buffer, length })
    }

    fn extend_unaligned(&mut self, slice: &[u8], offset: usize, length: usize) {
        // e.g.
        // [a, b, --101010]     <- to be extended
        // [00111111, 11010101] <- to extend
        // [a, b, 11101010, --001111] expected result

        let aligned_offset = offset / 8;
        let own_offset = self.length % 8;
        debug_assert_eq!(offset % 8, 0); // assumed invariant
        debug_assert!(own_offset != 0); // assumed invariant

        let bytes_len = length.saturating_add(7) / 8;
        let items = &slice[aligned_offset..aligned_offset + bytes_len];
        // self has some offset => we need to shift all `items`, and merge the first
        let buffer = self.buffer.as_mut_slice();
        let last = &mut buffer[buffer.len() - 1];

        // --101010 | 00111111 << 6 = 11101010
        // erase previous
        *last &= 0b11111111u8 >> (8 - own_offset); // unset before setting
        *last |= items[0] << own_offset;

        if length + own_offset <= 8 {
            // no new bytes needed
            self.length += length;
            return;
        }
        let additional = length - (8 - own_offset);

        let remaining = [items[items.len() - 1], 0];
        let bytes = items
            .windows(2)
            .chain(std::iter::once(remaining.as_ref()))
            .map(|w| merge_reversed(w[0], w[1], 8 - own_offset))
            .take(additional.saturating_add(7) / 8);
        self.buffer.extend_from_trusted_len_iter(bytes);

        self.length += length;
    }

    fn extend_aligned(&mut self, slice: &[u8], offset: usize, length: usize) {
        let aligned_offset = offset / 8;
        let bytes_len = length.saturating_add(7) / 8;
        let items = &slice[aligned_offset..aligned_offset + bytes_len];
        self.buffer.extend_from_slice(items);
        self.length += length;
    }

    /// Extends the [`MutableBitmap`] from a slice of bytes with optional offset.
    /// This is the fastest way to extend a [`MutableBitmap`].
    /// # Implementation
    /// When both [`MutableBitmap`]'s length and `offset` are both multiples of 8,
    /// this function performs a memcopy. Else, it extends [`MutableBitmap`] bit by bit.
    #[inline]
    pub fn extend_from_slice(&mut self, slice: &[u8], offset: usize, length: usize) {
        assert!(offset + length <= slice.len() * 8);
        if length == 0 {
            return;
        };
        let is_aligned = self.length % 8 == 0;
        let other_is_aligned = offset % 8 == 0;
        match (is_aligned, other_is_aligned) {
            (true, true) => self.extend_aligned(slice, offset, length),
            (false, true) => self.extend_unaligned(slice, offset, length),
            // todo: further optimize the other branches.
            _ => self.extend_from_trusted_len_iter(BitmapIter::new(slice, offset, length)),
        }
        // internal invariant:
        debug_assert_eq!(self.length.saturating_add(7) / 8, self.buffer.len());
    }

    /// Extends the [`MutableBitmap`] from a [`Bitmap`].
    #[inline]
    pub fn extend_from_bitmap(&mut self, bitmap: &Bitmap) {
        let (slice, offset, length) = bitmap.as_slice();
        self.extend_from_slice(slice, offset, length);
    }

    /// Returns the slice of bytes of this [`MutableBitmap`].
    /// Note that the last byte may not be fully used.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        let len = (self.length).saturating_add(7) / 8;
        &self.buffer[..len]
    }
}

impl Default for MutableBitmap {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> IntoIterator for &'a MutableBitmap {
    type Item = bool;
    type IntoIter = BitmapIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        BitmapIter::<'a>::new(&self.buffer, 0, self.length)
    }
}

impl<'a> MutableBitmap {
    /// constructs a new iterator over the values of [`MutableBitmap`].
    pub fn iter(&'a self) -> BitmapIter<'a> {
        BitmapIter::<'a>::new(&self.buffer, 0, self.length)
    }
}
