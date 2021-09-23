use std::{convert::AsRef, iter::FromIterator, sync::Arc, usize};

use crate::{trusted_len::TrustedLen, types::NativeType};

use super::bytes::Bytes;
use super::mutable::MutableBuffer;

/// [`Buffer`] is a contiguous memory region that can
/// be shared across thread boundaries.
/// The easiest way to think about `Buffer<T>` is being equivalent to
/// an immutable `Vec<T>`, with the following differences:
/// * `T` must be [`NativeType`]
/// * clone is `O(1)`
/// * memory is sharable across thread boundaries (it is under an `Arc`)
/// * it supports external allocated memory (FFI)
#[derive(Clone, PartialEq)]
pub struct Buffer<T: NativeType> {
    /// the internal byte buffer.
    data: Arc<Bytes<T>>,

    /// The offset into the buffer.
    offset: usize,

    // the length of the buffer. Given a region `data` of N bytes, [offset..offset+length] is visible
    // to this buffer.
    length: usize,
}

impl<T: NativeType> std::fmt::Debug for Buffer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, f)
    }
}

impl<T: NativeType> Default for Buffer<T> {
    #[inline]
    fn default() -> Self {
        MutableBuffer::new().into()
    }
}

impl<T: NativeType> Buffer<T> {
    /// Creates an empty [`Buffer`].
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new [`Buffer`] filled with zeros.
    #[inline]
    pub fn new_zeroed(length: usize) -> Self {
        MutableBuffer::from_len_zeroed(length).into()
    }

    /// Auxiliary method to create a new Buffer
    #[inline]
    pub fn from_bytes(bytes: Bytes<T>) -> Self {
        let length = bytes.len();
        Buffer {
            data: Arc::new(bytes),
            offset: 0,
            length,
        }
    }

    /// Returns the number of bytes in the buffer
    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns whether the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the byte slice stored in this buffer
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        &self.data[self.offset..self.offset + self.length]
    }

    /// Returns a new [Buffer] that is a slice of this buffer starting at `offset`.
    /// Doing so allows the same memory region to be shared between buffers.
    /// # Panics
    /// Panics iff `offset` is larger than `len`.
    #[inline]
    pub fn slice(self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        // Safety: we just checked bounds
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Returns a new [Buffer] that is a slice of this buffer starting at `offset`.
    /// Doing so allows the same memory region to be shared between buffers.
    /// # Safety
    /// The caller must ensure `offset + length <= self.len()`
    #[inline]
    pub unsafe fn slice_unchecked(mut self, offset: usize, length: usize) -> Self {
        self.offset += offset;
        self.length = length;
        self
    }

    /// Returns a pointer to the start of this buffer.
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        unsafe { self.data.ptr().as_ptr().add(self.offset) }
    }
}

impl<T: NativeType> Buffer<T> {
    /// Creates a [`Buffer`] from an [`Iterator`] with a trusted length.
    /// Prefer this to `collect` whenever possible, as it often enables auto-vectorization.
    /// # Example
    /// ```
    /// # use arrow2::buffer::Buffer;
    /// let v = vec![1u32];
    /// let iter = v.iter().map(|x| x * 2);
    /// let buffer = unsafe { Buffer::from_trusted_len_iter(iter) };
    /// assert_eq!(buffer.len(), 1)
    /// ```
    #[inline]
    pub fn from_trusted_len_iter<I: TrustedLen<Item = T>>(iterator: I) -> Self {
        MutableBuffer::from_trusted_len_iter(iterator).into()
    }

    /// # Safety
    /// This method assumes that the iterator's size is correct and is undefined behavior
    /// to use it on an iterator that reports an incorrect length.
    #[inline]
    pub fn try_from_trusted_len_iter<E, I: TrustedLen<Item = std::result::Result<T, E>>>(
        iterator: I,
    ) -> std::result::Result<Self, E> {
        Ok(MutableBuffer::try_from_trusted_len_iter(iterator)?.into())
    }

    /// Creates a [`Buffer`] from an [`Iterator`] with a trusted (upper) length.
    /// # Safety
    /// This method assumes that the iterator's size is correct and is undefined behavior
    /// to use it on an iterator that reports an incorrect length.
    #[inline]
    pub unsafe fn from_trusted_len_iter_unchecked<I: Iterator<Item = T>>(iterator: I) -> Self {
        MutableBuffer::from_trusted_len_iter_unchecked(iterator).into()
    }

    /// # Safety
    /// This method assumes that the iterator's size is correct and is undefined behavior
    /// to use it on an iterator that reports an incorrect length.
    #[inline]
    pub unsafe fn try_from_trusted_len_iter_unchecked<
        E,
        I: Iterator<Item = std::result::Result<T, E>>,
    >(
        iterator: I,
    ) -> std::result::Result<Self, E> {
        Ok(MutableBuffer::try_from_trusted_len_iter_unchecked(iterator)?.into())
    }
}

impl<T: NativeType, U: AsRef<[T]>> From<U> for Buffer<T> {
    #[inline]
    fn from(p: U) -> Self {
        MutableBuffer::from(p).into()
    }
}

impl<T: NativeType> std::ops::Deref for Buffer<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T: NativeType> FromIterator<T> for Buffer<T> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        MutableBuffer::from_iter(iter).into()
    }
}
