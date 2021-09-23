use std::iter::FromIterator;
use std::ptr::NonNull;
use std::usize;

use crate::trusted_len::TrustedLen;
use crate::types::{BitChunk, NativeType};

use super::bytes::{Bytes, Deallocation};
#[cfg(feature = "cache_aligned")]
use crate::vec::AlignedVec as Vec;

use super::immutable::Buffer;

/// A [`MutableBuffer`] is this crates' interface to store types that are byte-like such as `i32`.
/// It behaves like a [`Vec`] but can only hold types supported by the arrow format
/// (`u8-u64`, `i8-i128`, `f32,f64`, [`crate::types::days_ms`] and [`crate::types::months_days_ns`]).
/// When the feature `cache_aligned` is active, memory is allocated along cache lines and in multiple of 64 bytes.
/// A [`MutableBuffer`] can be converted to a [`Buffer`] via `.into`.
/// # Example
/// ```
/// # use arrow2::buffer::{Buffer, MutableBuffer};
/// let mut buffer = MutableBuffer::<u32>::new();
/// buffer.push(256);
/// buffer.extend_from_slice(&[1]);
/// assert_eq!(buffer.as_slice(), &[256, 1]);
/// let buffer: Buffer<u32> = buffer.into();
/// assert_eq!(buffer.as_slice(), &[256, 1])
/// ```
pub struct MutableBuffer<T: NativeType> {
    data: Vec<T>,
}

#[cfg(not(feature = "cache_aligned"))]
#[cfg_attr(docsrs, doc(cfg(not(feature = "cache_aligned"))))]
impl<T: NativeType> From<MutableBuffer<T>> for Vec<T> {
    fn from(data: MutableBuffer<T>) -> Self {
        data.data
    }
}

impl<T: NativeType> std::fmt::Debug for MutableBuffer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, f)
    }
}

impl<T: NativeType> PartialEq for MutableBuffer<T> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T: NativeType> MutableBuffer<T> {
    /// Creates an empty [`MutableBuffer`]. This does not allocate in the heap.
    #[inline]
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    /// Allocate a new [`MutableBuffer`] with initial capacity to be at least `capacity`.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    /// Takes ownership of [`Vec`].
    #[cfg(not(feature = "cache_aligned"))]
    #[cfg_attr(docsrs, doc(cfg(not(feature = "cache_aligned"))))]
    #[inline]
    pub fn from_vec(data: Vec<T>) -> Self {
        Self { data }
    }

    /// Allocates a new [MutableBuffer] with `len` and capacity to be at least `len`
    /// where data is zeroed.
    /// # Example
    /// ```
    /// # use arrow2::buffer::{Buffer, MutableBuffer};
    /// let mut buffer = MutableBuffer::<u8>::from_len_zeroed(127);
    /// assert_eq!(buffer.len(), 127);
    /// assert!(buffer.capacity() >= 127);
    /// let data = buffer.as_mut_slice();
    /// assert_eq!(data[126], 0u8);
    /// ```
    #[inline]
    pub fn from_len_zeroed(len: usize) -> Self {
        #[cfg(not(feature = "cache_aligned"))]
        let data = vec![T::default(); len];
        #[cfg(feature = "cache_aligned")]
        let data = Vec::from_len_zeroed(len);
        Self { data }
    }

    /// Ensures that this buffer has at least `self.len + additional` bytes. This re-allocates iff
    /// `self.len + additional > capacity`.
    /// # Example
    /// ```
    /// # use arrow2::buffer::{Buffer, MutableBuffer};
    /// let mut buffer = MutableBuffer::<u8>::new();
    /// buffer.reserve(253); // allocates for the first time
    /// (0..253u8).for_each(|i| buffer.push(i)); // no reallocation
    /// let buffer: Buffer<u8> = buffer.into();
    /// assert_eq!(buffer.len(), 253);
    /// ```
    // For performance reasons, this must be inlined so that the `if` is executed inside the caller, and not as an extra call that just
    // exits.
    #[inline(always)]
    pub fn reserve(&mut self, additional: usize) {
        self.data.reserve(additional)
    }

    /// Resizes the buffer, either truncating its contents (with no change in capacity), or
    /// growing it (potentially reallocating it) and writing `value` in the newly available bytes.
    /// # Example
    /// ```
    /// # use arrow2::buffer::{Buffer, MutableBuffer};
    /// let mut buffer = MutableBuffer::<u8>::new();
    /// buffer.resize(253, 2); // allocates for the first time
    /// assert_eq!(buffer.as_slice()[252], 2u8);
    /// ```
    // For performance reasons, this must be inlined so that the `if` is executed inside the caller, and not as an extra call that just
    // exits.
    #[inline(always)]
    pub fn resize(&mut self, new_len: usize, value: T) {
        self.data.resize(new_len, value)
    }

    /// Returns whether this buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the length (the number of items) in this buffer.
    /// The invariant `buffer.len() <= buffer.capacity()` is always upheld.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns the total capacity in this buffer.
    /// The invariant `buffer.len() <= buffer.capacity()` is always upheld.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    /// Clear all existing data from this buffer.
    #[inline]
    pub fn clear(&mut self) {
        self.data.clear()
    }

    /// Shortens the buffer.
    /// If `len` is greater or equal to the buffers' current length, this has no effect.
    #[inline]
    pub fn truncate(&mut self, len: usize) {
        self.data.truncate(len)
    }

    /// Returns the data stored in this buffer as a slice.
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        self.data.as_slice()
    }

    /// Returns the data stored in this buffer as a mutable slice.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        self.data.as_mut_slice()
    }

    /// Returns a raw pointer to this buffer's internal memory
    /// This pointer is guaranteed to be aligned along cache-lines.
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.data.as_ptr()
    }

    /// Returns a mutable raw pointer to this buffer's internal memory
    /// This pointer is guaranteed to be aligned along cache-lines.
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.data.as_mut_ptr()
    }

    /// Extends this buffer from a slice of items, increasing its capacity if needed.
    /// # Example
    /// ```
    /// # use arrow2::buffer::MutableBuffer;
    /// let mut buffer = MutableBuffer::new();
    /// buffer.extend_from_slice(&[2u32, 0]);
    /// assert_eq!(buffer.len(), 2)
    /// ```
    #[inline]
    pub fn extend_from_slice(&mut self, items: &[T]) {
        self.data.extend_from_slice(items)
    }

    /// Pushes a new item to the buffer, increasing its capacity if needed.
    /// # Example
    /// ```
    /// # use arrow2::buffer::MutableBuffer;
    /// let mut buffer = MutableBuffer::new();
    /// buffer.push(256u32);
    /// assert_eq!(buffer.len(), 1)
    /// ```
    #[inline]
    pub fn push(&mut self, item: T) {
        self.data.push(item)
    }

    /// Extends the buffer with a new item without checking for sufficient capacity
    /// Safety
    /// Caller must ensure that `self.capacity() - self.len() >= 1`
    #[inline]
    pub(crate) unsafe fn push_unchecked(&mut self, item: T) {
        let dst = self.as_mut_ptr().add(self.len());
        std::ptr::write(dst, item);
        self.data.set_len(self.data.len() + 1);
    }

    /// Sets the length of this buffer.
    /// # Safety
    /// The caller must uphold the following invariants:
    /// * ensure no reads are performed on any
    ///     item within `[len, capacity - len]`
    /// * ensure `len <= self.capacity()`
    #[inline]
    pub unsafe fn set_len(&mut self, len: usize) {
        debug_assert!(len <= self.capacity());
        self.data.set_len(len);
    }

    /// Extends this buffer by `additional` items of value `value`.
    #[inline]
    pub fn extend_constant(&mut self, additional: usize, value: T) {
        self.resize(self.len() + additional, value)
    }

    /// Shrinks the capacity of the [`MutableBuffer`] to fit its current length.
    /// The new capacity will be a multiple of 64 bytes.
    ///
    /// # Example
    /// ```
    /// # use arrow2::buffer::MutableBuffer;
    ///
    /// let mut buffer = MutableBuffer::<u64>::with_capacity(16);
    /// assert_eq!(buffer.capacity(), 16);
    /// buffer.push(1);
    /// buffer.push(2);
    ///
    /// buffer.shrink_to_fit();
    /// assert!(buffer.capacity() == 2);
    /// ```
    pub fn shrink_to_fit(&mut self) {
        self.data.shrink_to_fit();
    }
}

impl<A: NativeType> Extend<A> for MutableBuffer<A> {
    fn extend<T: IntoIterator<Item = A>>(&mut self, iter: T) {
        self.data.extend(iter)
    }
}

impl<T: NativeType> MutableBuffer<T> {
    /// Extends `self` from a [`TrustedLen`] iterator.
    #[inline]
    pub fn extend_from_trusted_len_iter<I: TrustedLen<Item = T>>(&mut self, iterator: I) {
        unsafe { self.extend_from_trusted_len_iter_unchecked(iterator) }
    }

    /// Extends `self` from an iterator.
    /// # Safety
    /// This method assumes that the iterator's size is correct and is undefined behavior
    /// to use it on an iterator that reports an incorrect length.
    // This inline has been validated to offer 50% improvement in operations like `take`.
    #[inline]
    pub unsafe fn extend_from_trusted_len_iter_unchecked<I: Iterator<Item = T>>(
        &mut self,
        iterator: I,
    ) {
        let (_, upper) = iterator.size_hint();
        let upper = upper.expect("trusted_len_iter requires an upper limit");
        let len = upper;

        let self_len = self.len();

        self.reserve(len);
        let mut dst = self.as_mut_ptr().add(self_len);
        for item in iterator {
            // note how there is no reserve here (compared with `extend_from_iter`)
            std::ptr::write(dst, item);
            dst = dst.add(1);
        }
        assert_eq!(
            dst.offset_from(self.as_ptr().add(self_len)) as usize,
            upper,
            "Trusted iterator length was not accurately reported"
        );
        self.set_len(self_len + len);
    }

    /// Creates a [`MutableBuffer`] from an [`Iterator`] with a trusted (upper) length.
    /// Prefer this to `collect` whenever possible, as it is faster ~60% faster.
    /// # Example
    /// ```
    /// # use arrow2::buffer::MutableBuffer;
    /// let v = vec![1u32];
    /// let iter = v.iter().map(|x| x * 2);
    /// let buffer = MutableBuffer::from_trusted_len_iter(iter);
    /// assert_eq!(buffer.len(), 1)
    /// ```
    /// # Safety
    /// This method assumes that the iterator's size is correct and is undefined behavior
    /// to use it on an iterator that reports an incorrect length.
    // This implementation is required for two reasons:
    // 1. there is no trait `TrustedLen` in stable rust and therefore
    //    we can't specialize `extend` for `TrustedLen` like `Vec` does.
    // 2. `from_trusted_len_iter` is faster.
    #[inline]
    pub fn from_trusted_len_iter<I: Iterator<Item = T> + TrustedLen>(iterator: I) -> Self {
        let mut buffer = MutableBuffer::new();
        buffer.extend_from_trusted_len_iter(iterator);
        buffer
    }

    /// Creates a [`MutableBuffer`] from an [`Iterator`] with a trusted (upper) length.
    /// Prefer this to `collect` whenever possible, as it is faster ~60% faster.
    /// # Safety
    /// This method assumes that the iterator's size is correct and is undefined behavior
    /// to use it on an iterator that reports an incorrect length.
    // This implementation is required for two reasons:
    // 1. there is no trait `TrustedLen` in stable rust and therefore
    //    we can't specialize `extend` for `TrustedLen` like `Vec` does.
    // 2. `from_trusted_len_iter` is faster.
    #[inline]
    pub unsafe fn from_trusted_len_iter_unchecked<I: Iterator<Item = T>>(iterator: I) -> Self {
        let mut buffer = MutableBuffer::new();
        buffer.extend_from_trusted_len_iter_unchecked(iterator);
        buffer
    }

    /// Creates a [`MutableBuffer`] from a fallible [`TrustedLen`] iterator.
    #[inline]
    pub fn try_from_trusted_len_iter<E, I: TrustedLen<Item = std::result::Result<T, E>>>(
        iterator: I,
    ) -> std::result::Result<Self, E> {
        unsafe { Self::try_from_trusted_len_iter_unchecked(iterator) }
    }

    /// Creates a [`MutableBuffer`] from an [`Iterator`] with a trusted (upper) length or errors
    /// if any of the items of the iterator is an error.
    /// Prefer this to `collect` whenever possible, as it is faster ~60% faster.
    /// The only difference between this and [`Self::try_from_trusted_len_iter`] is that this works
    /// on any iterator, while `try_from_trusted_len_iter` requires the iterator to implement the trait
    /// [`TrustedLen`], which not every iterator currently implements due to limitations of the Rust compiler.
    /// # Safety
    /// This method assumes that the iterator's size is correct and is undefined behavior
    /// to use it on an iterator that reports an incorrect length.
    // This inline has been validated to offer 50% improvement in operations like `take`.
    #[inline]
    pub unsafe fn try_from_trusted_len_iter_unchecked<
        E,
        I: Iterator<Item = std::result::Result<T, E>>,
    >(
        iterator: I,
    ) -> std::result::Result<Self, E> {
        let (_, upper) = iterator.size_hint();
        let upper = upper.expect("try_from_trusted_len_iter requires an upper limit");
        let len = upper;

        let mut buffer = MutableBuffer::with_capacity(len);

        let mut dst = buffer.as_mut_ptr();
        for item in iterator {
            std::ptr::write(dst, item?);
            dst = dst.add(1);
        }
        assert_eq!(
            dst.offset_from(buffer.as_ptr()) as usize,
            upper,
            "Trusted iterator length was not accurately reported"
        );
        buffer.set_len(len);
        Ok(buffer)
    }
}

impl<T: NativeType> FromIterator<T> for MutableBuffer<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let data = Vec::from_iter(iter);
        Self { data }
    }
}

impl<T: NativeType> Default for MutableBuffer<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: NativeType> std::ops::Deref for MutableBuffer<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        &self.data
    }
}

impl<T: NativeType> std::ops::DerefMut for MutableBuffer<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [T] {
        &mut self.data
    }
}

impl<T: NativeType, P: AsRef<[T]>> From<P> for MutableBuffer<T> {
    #[inline]
    fn from(slice: P) -> Self {
        let mut buffer = MutableBuffer::new();
        buffer.extend_from_slice(slice.as_ref());
        buffer
    }
}

impl<T: NativeType> From<MutableBuffer<T>> for Buffer<T> {
    #[inline]
    fn from(buffer: MutableBuffer<T>) -> Self {
        Self::from_bytes(buffer.into())
    }
}

impl<T: NativeType> From<MutableBuffer<T>> for Bytes<T> {
    #[inline]
    fn from(buffer: MutableBuffer<T>) -> Self {
        let mut data = buffer.data;
        let ptr = NonNull::new(data.as_mut_ptr()).unwrap();
        let len = data.len();
        let capacity = data.capacity();

        let result = unsafe { Bytes::new(ptr, len, Deallocation::Native(capacity)) };
        // so that the memory region is not deallocated.
        std::mem::forget(data);
        result
    }
}

impl MutableBuffer<u8> {
    /// Creates a [`MutableBuffer<u8>`] from an iterator of `u64`.
    #[inline]
    pub fn from_chunk_iter<T: BitChunk, I: TrustedLen<Item = T>>(iter: I) -> Self {
        // TrustedLen
        unsafe { Self::from_chunk_iter_unchecked(iter) }
    }

    /// # Safety
    /// This method assumes that the iterator's size is correct and is undefined behavior
    /// to use it on an iterator that reports an incorrect length.
    #[inline]
    pub unsafe fn from_chunk_iter_unchecked<T: BitChunk, I: Iterator<Item = T>>(
        iterator: I,
    ) -> Self {
        let (_, upper) = iterator.size_hint();
        let upper = upper.expect("try_from_trusted_len_iter requires an upper limit");
        let len = upper * std::mem::size_of::<T>();

        let mut buffer = MutableBuffer::with_capacity(len);

        let mut dst = buffer.as_mut_ptr();
        for item in iterator {
            let bytes = item.to_ne_bytes();
            for i in 0..std::mem::size_of::<T>() {
                std::ptr::write(dst, bytes[i]);
                dst = dst.add(1);
            }
        }
        assert_eq!(
            dst.offset_from(buffer.as_ptr()) as usize,
            len,
            "Trusted iterator length was not accurately reported"
        );
        buffer.set_len(len);
        buffer
    }
}

// This is sound because `NativeType` is `Send+Sync`, and
// `MutableBuffer` has the invariants of `Vec<T>` (which is `Send+Sync`)
unsafe impl<T: NativeType> Send for MutableBuffer<T> {}
unsafe impl<T: NativeType> Sync for MutableBuffer<T> {}
