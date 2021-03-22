use std::sync::Arc;
use std::{convert::AsRef, usize};
use std::{fmt::Debug, iter::FromIterator};

use crate::types::NativeType;

use super::bytes::Bytes;
use super::mutable::MutableBuffer;

/// Buffer represents a contiguous memory region that can be shared with other buffers and across
/// thread boundaries.
#[derive(Clone, PartialEq, Debug)]
pub struct Buffer<T: NativeType> {
    /// the internal byte buffer.
    data: Arc<Bytes<T>>,

    /// The offset into the buffer.
    offset: usize,

    // the length of the buffer. Given a region `data` of N bytes, [offset..offset+length] is visible
    // to this buffer.
    length: usize,
}

impl<T: NativeType> Default for Buffer<T> {
    fn default() -> Self {
        MutableBuffer::new().into()
    }
}

impl<T: NativeType> Buffer<T> {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

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
    pub fn slice(mut self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
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
    /// Creates a [`Buffer`] from an [`Iterator`] with a trusted (upper) length.
    /// Prefer this to `collect` whenever possible, as it often enables auto-vectorization.
    /// # Example
    /// ```
    /// # use arrow2::buffer::Buffer;
    /// let v = vec![1u32];
    /// let iter = v.iter().map(|x| x * 2);
    /// let buffer = unsafe { Buffer::from_trusted_len_iter(iter) };
    /// assert_eq!(buffer.len(), 1)
    /// ```
    /// # Safety
    /// This method assumes that the iterator's size is correct and is undefined behavior
    /// to use it on an iterator that reports an incorrect length.
    #[inline]
    pub unsafe fn from_trusted_len_iter<I: Iterator<Item = T>>(iterator: I) -> Self {
        MutableBuffer::from_trusted_len_iter(iterator).into()
    }

    /// # Safety
    /// This method assumes that the iterator's size is correct and is undefined behavior
    /// to use it on an iterator that reports an incorrect length.
    #[inline]
    pub unsafe fn try_from_trusted_len_iter<E, I: Iterator<Item = std::result::Result<T, E>>>(
        iterator: I,
    ) -> std::result::Result<Self, E> {
        Ok(MutableBuffer::try_from_trusted_len_iter(iterator)?.into())
    }
}

impl<T: NativeType, U: AsRef<[T]>> From<U> for Buffer<T> {
    #[inline]
    fn from(p: U) -> Self {
        // allocate aligned memory buffer
        let slice = p.as_ref();
        let len = slice.len();
        let mut buffer = MutableBuffer::with_capacity(len);
        buffer.extend_from_slice(slice);
        buffer.into()
    }
}

impl<T: NativeType> FromIterator<T> for Buffer<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        MutableBuffer::from_iter(iter).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let buffer = Buffer::<i32>::new();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.is_empty(), true);
    }

    #[test]
    fn test_new_zeroed() {
        let buffer = Buffer::<i32>::new_zeroed(2);
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.is_empty(), false);
        assert_eq!(buffer.as_slice(), &[0, 0]);
    }

    #[test]
    fn test_from_slice() {
        let buffer = Buffer::<i32>::from(&[0, 1, 2]);
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.as_slice(), &[0, 1, 2]);
    }

    #[test]
    fn test_slice() {
        let buffer = Buffer::<i32>::from(&[0, 1, 2, 3]);
        let buffer = buffer.slice(1, 2);
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.as_slice(), &[1, 2]);
    }

    #[test]
    fn test_from_iter() {
        let buffer = (0..3).collect::<Buffer<i32>>();
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.as_slice(), &[0, 1, 2]);
    }

    #[test]
    fn test_from_trusted_len_iter() {
        let buffer = unsafe { Buffer::<i32>::from_trusted_len_iter(0..3) };
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.as_slice(), &[0, 1, 2]);
    }

    #[test]
    fn test_try_from_trusted_len_iter() {
        let iter = (0..3).map(Result::<_, String>::Ok);
        let buffer = unsafe { Buffer::<i32>::try_from_trusted_len_iter(iter) }.unwrap();
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.as_slice(), &[0, 1, 2]);
    }

    #[test]
    fn test_as_ptr() {
        let buffer = Buffer::<i32>::from(&[0, 1, 2, 3]);
        let buffer = buffer.slice(1, 2);
        let ptr = buffer.as_ptr();
        assert_eq!(unsafe { *ptr }, 1);
    }
}
