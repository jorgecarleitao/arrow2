use either::Either;
use std::{iter::FromIterator, sync::Arc, usize};

use crate::types::NativeType;

use super::bytes::Bytes;

/// [`Buffer`] is a contiguous memory region that can be shared across thread boundaries.
///
/// The easiest way to think about `Buffer<T>` is being equivalent to
/// a `Arc<Vec<T>>`, with the following differences:
/// * `T` must be [`NativeType`]
/// * slicing the buffer is `O(1)`
/// * it supports external allocated memory (via FFI)
///
/// The easiest way to create one is to use its implementation of `From` a `Vec`.
///
/// # Examples
/// ```
/// use arrow2::buffer::Buffer;
///
/// let buffer: Buffer<u32> = vec![1, 2, 3].into();
/// assert_eq!(buffer.as_ref(), [1, 2, 3].as_ref());
///
/// // it supports copy-on-write semantics (i.e. back to a `Vec`)
/// let vec: Vec<u32> = buffer.into_mut().right().unwrap();
/// assert_eq!(vec, vec![1, 2, 3]);
///
/// // cloning and slicing is `O(1)` (data is shared)
/// let buffer: Buffer<u32> = vec![1, 2, 3].into();
/// let slice = buffer.clone().slice(1, 1);
/// assert_eq!(slice.as_ref(), [2].as_ref());
/// // no longer possible to get a vec since `slice` and `buffer` share data
/// let same: Buffer<u32> = buffer.into_mut().left().unwrap();
/// ```
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

    /// Converts this [`Buffer`] to [`Vec`], returning itself if the conversion
    /// is not possible
    ///
    /// This operation returns a [`Vec`] iff this [`Buffer`]:
    /// * is not an offsetted slice of another [`Buffer`]
    /// * has not been cloned (i.e. [`Arc`]`::get_mut` yields [`Some`])
    /// * has not been imported from the c data interface (FFI)
    pub fn into_mut(mut self) -> Either<Self, Vec<T>> {
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

    /// Converts this [`Buffer`] to a [`Vec`], cloning the data if needed, also
    /// known as clone-on-write semantics.
    ///
    /// This function is O(1) under the same conditions that [`Self::into_mut`] returns `Vec`.
    pub fn make_mut(self) -> Vec<T> {
        match self.into_mut() {
            Either::Left(data) => data.as_ref().to_vec(),
            Either::Right(data) => data,
        }
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
