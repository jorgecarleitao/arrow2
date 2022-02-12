use either::Either;
use std::iter::FromIterator;
use std::sync::Arc;

use crate::{buffer::bytes::Bytes, trusted_len::TrustedLen};

use super::{
    utils::{count_zeros, fmt, get_bit, get_bit_unchecked, BitChunk, BitChunks, BitmapIter},
    MutableBitmap,
};

/// An immutable container whose API is optimized to handle bitmaps. All quantities on this
/// container's API are measured in bits.
/// # Implementation
/// * memory on this container is sharable across thread boundaries
/// * Cloning [`Bitmap`] is `O(1)`
/// * Slicing [`Bitmap`] is `O(1)`
#[derive(Clone)]
pub struct Bitmap {
    bytes: Arc<Bytes<u8>>,
    // both are measured in bits. They are used to bound the bitmap to a region of Bytes.
    offset: usize,
    length: usize,
    // this is a cache: it must be computed on initialization
    null_count: usize,
}

impl std::fmt::Debug for Bitmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (bytes, offset, len) = self.as_slice();
        fmt(bytes, offset, len, f)
    }
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

    /// Initializes an new [`Bitmap`] filled with unset values.
    #[inline]
    pub fn new_zeroed(length: usize) -> Self {
        MutableBitmap::from_len_zeroed(length).into()
    }

    /// Returns the length of the [`Bitmap`].
    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns whether [`Bitmap`] is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Creates a new [`Bitmap`] from [`Bytes`] and a length.
    /// # Panic
    /// Panics iff `length <= bytes.len() * 8`
    #[inline]
    pub(crate) fn from_bytes(bytes: Bytes<u8>, length: usize) -> Self {
        assert!(length <= bytes.len() * 8);
        let null_count = count_zeros(&bytes, 0, length);
        Self {
            length,
            offset: 0,
            bytes: Arc::new(bytes),
            null_count,
        }
    }

    /// Creates a new [`Bitmap`] from [`Vec`] and a length.
    /// This function is `O(1)`
    /// # Panic
    /// Panics iff `length <= buffer.len() * 8`
    #[inline]
    pub fn from_u8_vec(vec: Vec<u8>, length: usize) -> Self {
        Bitmap::from_bytes(vec.into(), length)
    }

    /// Creates a new [`Bitmap`] from a slice and length.
    /// # Panic
    /// Panics iff `length <= bytes.len() * 8`
    #[inline]
    pub fn from_u8_slice<T: AsRef<[u8]>>(buffer: T, length: usize) -> Self {
        let buffer = Vec::<u8>::from(buffer.as_ref());
        Bitmap::from_u8_vec(buffer, length)
    }

    /// Counts the nulls (unset bits) starting from `offset` bits and for `length` bits.
    #[inline]
    pub fn null_count_range(&self, offset: usize, length: usize) -> usize {
        count_zeros(&self.bytes, self.offset + offset, length)
    }

    /// Returns the number of unset bits on this [`Bitmap`].
    #[inline]
    pub fn null_count(&self) -> usize {
        self.null_count
    }

    /// Slices `self`, offsetting by `offset` and truncating up to `length` bits.
    /// # Panic
    /// Panics iff `self.offset + offset + length >= self.bytes.len() * 8`, i.e. if the offset and `length`
    /// exceeds the allocated capacity of `self`.
    #[inline]
    pub fn slice(self, offset: usize, length: usize) -> Self {
        assert!(offset + length <= self.length);
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Slices `self`, offseting by `offset` and truncating up to `length` bits.
    /// # Safety
    /// The caller must ensure that `self.offset + offset + length <= self.len()`
    #[inline]
    pub unsafe fn slice_unchecked(mut self, offset: usize, length: usize) -> Self {
        // count the smallest chunk
        if length < self.length / 2 {
            // count the null values in the slice
            self.null_count = count_zeros(&self.bytes, offset, length);
        } else {
            // subtract the null count of the chunks we slice off
            let start_end = self.offset + offset + length;
            let head_count = count_zeros(&self.bytes, self.offset, offset);
            let tail_count = count_zeros(&self.bytes, start_end, self.length - length - offset);
            self.null_count -= head_count + tail_count;
        }
        self.offset += offset;
        self.length = length;
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

    /// Returns a pointer to the start of this [`Bitmap`] (ignores `offsets`)
    /// This pointer is allocated iff `self.len() > 0`.
    pub(crate) fn as_ptr(&self) -> std::ptr::NonNull<u8> {
        self.bytes.ptr()
    }

    /// Returns a pointer to the start of this [`Bitmap`] (ignores `offsets`)
    /// This pointer is allocated iff `self.len() > 0`.
    pub(crate) fn offset(&self) -> usize {
        self.offset
    }

    /// Converts this [`Bitmap`] to [`MutableBitmap`], returning itself if the conversion
    /// is not possible
    ///
    /// This operation returns a [`MutableBitmap`] iff:
    /// * this [`Bitmap`] is not an offsetted slice of another [`Bitmap`]
    /// * this [`Bitmap`] has not been cloned (i.e. [`Arc`]`::get_mut` yields [`Some`])
    /// * this [`Bitmap`] was not imported from the c data interface (FFI)
    pub fn into_mut(mut self) -> Either<Self, MutableBitmap> {
        match (
            self.offset,
            Arc::get_mut(&mut self.bytes).and_then(|b| b.get_vec()),
        ) {
            (0, Some(v)) => {
                let data = std::mem::take(v);
                Either::Right(MutableBitmap::from_vec(data, self.length))
            }
            _ => Either::Left(self),
        }
    }
}

impl<P: AsRef<[bool]>> From<P> for Bitmap {
    fn from(slice: P) -> Self {
        Self::from_trusted_len_iter(slice.as_ref().iter().copied())
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
}

impl Bitmap {
    /// Creates a new [`Bitmap`] from an iterator of booleans.
    /// # Safety
    /// The iterator must report an accurate length.
    #[inline]
    pub unsafe fn from_trusted_len_iter_unchecked<I: Iterator<Item = bool>>(iterator: I) -> Self {
        MutableBitmap::from_trusted_len_iter_unchecked(iterator).into()
    }

    /// Creates a new [`Bitmap`] from an iterator of booleans.
    #[inline]
    pub fn from_trusted_len_iter<I: TrustedLen<Item = bool>>(iterator: I) -> Self {
        MutableBitmap::from_trusted_len_iter(iterator).into()
    }

    /// Creates a new [`Bitmap`] from a fallible iterator of booleans.
    #[inline]
    pub fn try_from_trusted_len_iter<E, I: TrustedLen<Item = std::result::Result<bool, E>>>(
        iterator: I,
    ) -> std::result::Result<Self, E> {
        Ok(MutableBitmap::try_from_trusted_len_iter(iterator)?.into())
    }

    /// Creates a new [`Bitmap`] from a fallible iterator of booleans.
    /// # Safety
    /// The iterator must report an accurate length.
    #[inline]
    pub unsafe fn try_from_trusted_len_iter_unchecked<
        E,
        I: Iterator<Item = std::result::Result<bool, E>>,
    >(
        iterator: I,
    ) -> std::result::Result<Self, E> {
        Ok(MutableBitmap::try_from_trusted_len_iter_unchecked(iterator)?.into())
    }
}

impl Bitmap {
    /// Returns the byte slice of this Bitmap.
    ///
    /// The returned tuple contains:
    /// .1 -> The byte slice, truncated to the start of the first bit. So the start of the slice
    ///       is within the first 8 bits.
    /// .2 -> The start offset in bits, given what described above `0 <= offsets < 8`.
    /// .3 -> The length in bits.
    #[inline]
    pub fn as_slice(&self) -> (&[u8], usize, usize) {
        let start = self.offset / 8;
        let len = (self.offset % 8 + self.length).saturating_add(7) / 8;
        (
            &self.bytes[start..start + len],
            self.offset % 8,
            self.length,
        )
    }
}

impl<'a> IntoIterator for &'a Bitmap {
    type Item = bool;
    type IntoIter = BitmapIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        BitmapIter::<'a>::new(&self.bytes, self.offset, self.length)
    }
}

impl<'a> Bitmap {
    /// constructs a new iterator
    pub fn iter(&'a self) -> BitmapIter<'a> {
        BitmapIter::<'a>::new(&self.bytes, self.offset, self.length)
    }
}
