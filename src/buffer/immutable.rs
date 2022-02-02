use either::Either;
use std::{iter::FromIterator, sync::Arc, usize};

use crate::{trusted_len::TrustedLen, types::NativeType};

use super::bytes::Bytes;

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
        Vec::new().into()
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
        vec![T::default(); length].into()
    }

    /// Takes ownership of [`Vec`].
    /// # Implementation
    /// This function is `O(1)`
    #[inline]
    pub fn from_slice<R: AsRef<[T]>>(data: R) -> Self {
        data.as_ref().to_vec().into()
    }

    /// Auxiliary method to create a new Buffer
    pub(crate) fn from_bytes(bytes: Bytes<T>) -> Self {
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
        // Safety:
        // invariant of this struct `offset + length <= data.len()`
        debug_assert!(self.offset + self.length <= self.data.len());
        unsafe {
            self.data
                .get_unchecked(self.offset..self.offset + self.length)
        }
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
    pub(crate) fn as_ptr(&self) -> std::ptr::NonNull<T> {
        self.data.ptr()
    }

    /// Returns the offset of this buffer.
    #[inline]
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Try to get the inner data as a mutable [`Vec<T>`].
    /// This succeeds iff:
    /// * This data was allocated by Rust (i.e. it does not come from the C data interface)
    /// * This region is not being shared any other struct.
    /// * This buffer has no offset
    pub fn get_vec(mut self) -> Either<Self, Vec<T>> {
        if self.offset != 0 {
            Either::Left(self)
        } else {
            match Arc::get_mut(&mut self.data).and_then(|b| b.get_vec()) {
                Some(v) => {
                    let data = std::mem::take(v);
                    Either::Right(data)
                }
                None => Either::Left(self),
            }
        }
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
        iterator.collect::<Vec<_>>().into()
    }

    /// # Safety
    /// This method assumes that the iterator's size is correct and is undefined behavior
    /// to use it on an iterator that reports an incorrect length.
    #[inline]
    pub fn try_from_trusted_len_iter<E, I: TrustedLen<Item = std::result::Result<T, E>>>(
        iterator: I,
    ) -> std::result::Result<Self, E> {
        Ok(iterator.collect::<std::result::Result<Vec<_>, E>>()?.into())
    }

    /// Creates a [`Buffer`] from an [`Iterator`] with a trusted (upper) length.
    /// # Safety
    /// This method assumes that the iterator's size is correct and is undefined behavior
    /// to use it on an iterator that reports an incorrect length.
    #[inline]
    pub unsafe fn from_trusted_len_iter_unchecked<I: Iterator<Item = T>>(iterator: I) -> Self {
        iterator.collect::<Vec<_>>().into()
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
        Ok(iterator.collect::<std::result::Result<Vec<_>, E>>()?.into())
    }
}

impl<T: NativeType> From<Vec<T>> for Buffer<T> {
    #[inline]
    fn from(p: Vec<T>) -> Self {
        let bytes: Bytes<T> = p.into();
        Self {
            offset: 0,
            length: bytes.len(),
            data: Arc::new(bytes),
        }
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
        Vec::from_iter(iter).into()
    }
}
