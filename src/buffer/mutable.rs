use std::iter::FromIterator;
use std::ptr::NonNull;
use std::usize;
use std::{fmt::Debug, mem::size_of};

use crate::types::NativeType;
use crate::{alloc, trusted_len::TrustedLen};

use super::{
    bytes::{Bytes, Deallocation},
    util,
};

use super::immutable::Buffer;

#[inline]
fn capacity_multiple_of_64<T: NativeType>(capacity: usize) -> usize {
    util::round_upto_multiple_of_64(capacity * size_of::<T>()) / size_of::<T>()
}

/// A [`MutableBuffer`] is Arrow's interface to build a [`Buffer`] out of items, slices and iterators.
/// [`Buffer`]s created from [`MutableBuffer`] (via `into`) are guaranteed to have its pointer aligned
/// along cache lines and in multiple of 64 bytes.
/// Use [MutableBuffer::push] to insert an item, [MutableBuffer::extend_from_slice]
/// to insert many items, and `into` to convert it to [`Buffer`].
/// # Example
/// ```
/// # use arrow2::buffer::{Buffer, MutableBuffer};
/// let mut buffer = MutableBuffer::<u32>::new();
/// buffer.push(256);
/// buffer.extend_from_slice(&[1]);
/// let buffer: Buffer<u32> = buffer.into();
/// assert_eq!(buffer.as_slice(), &[256, 1])
/// ```
#[derive(Debug)]
pub struct MutableBuffer<T: NativeType> {
    // dangling iff capacity = 0
    ptr: NonNull<T>,
    // invariant: len <= capacity
    len: usize,
    capacity: usize,
}

impl<T: NativeType> MutableBuffer<T> {
    /// Creates an empty [`MutableBuffer`]. This does not allocate in the heap.
    #[inline]
    pub fn new() -> Self {
        let ptr = alloc::allocate_aligned(0);
        Self {
            ptr,
            len: 0,
            capacity: 0,
        }
    }

    /// Allocate a new [`MutableBuffer`] with initial capacity to be at least `capacity`.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity_multiple_of_64::<T>(capacity);
        let ptr = alloc::allocate_aligned(capacity);
        Self {
            ptr,
            len: 0,
            capacity,
        }
    }

    /// Allocates a new [MutableBuffer] with `len` and capacity to be at least `len` where
    /// all bytes are guaranteed to be `0u8`.
    /// # Example
    /// ```
    /// # use arrow2::buffer::{Buffer, MutableBuffer};
    /// let mut buffer = MutableBuffer::<u8>::from_len_zeroed(127);
    /// assert_eq!(buffer.len(), 127);
    /// assert!(buffer.capacity() >= 127);
    /// let data = buffer.as_slice_mut();
    /// assert_eq!(data[126], 0u8);
    /// ```
    #[inline]
    pub fn from_len_zeroed(len: usize) -> Self {
        let new_capacity = capacity_multiple_of_64::<T>(len);
        let ptr = alloc::allocate_aligned_zeroed(new_capacity);
        Self {
            ptr,
            len,
            capacity: new_capacity,
        }
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
        let required_cap = self.len + additional;
        if required_cap > self.capacity {
            // JUSTIFICATION
            //  Benefit
            //      necessity
            //  Soundness
            //      `self.data` is valid for `self.capacity`.
            let (ptr, new_capacity) = unsafe { reallocate(self.ptr, self.capacity, required_cap) };
            self.ptr = ptr;
            self.capacity = new_capacity;
        }
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
        if new_len > self.len {
            if self.capacity == 0 && value == T::default() {
                // edge case where the allocate
                let required_cap = capacity_multiple_of_64::<T>(new_len);
                let ptr = alloc::allocate_aligned_zeroed(required_cap);
                self.ptr = ptr;
                self.capacity = required_cap;
                self.len = new_len;
                return;
            }

            let diff = new_len - self.len;
            self.reserve(diff);
            unsafe {
                // write the value
                let mut ptr = self.ptr.as_ptr().add(self.len);
                (0..diff).for_each(|_| {
                    std::ptr::write(ptr, value);
                    ptr = ptr.add(1);
                })
            }
        }
        // this truncates the buffer when new_len < self.len
        self.len = new_len;
    }

    /// Returns whether this buffer is empty or not.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the length (the number of bytes written) in this buffer.
    /// The invariant `buffer.len() <= buffer.capacity()` is always upheld.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns the total capacity in this buffer.
    /// The invariant `buffer.len() <= buffer.capacity()` is always upheld.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Clear all existing data from this buffer.
    pub fn clear(&mut self) {
        self.len = 0
    }

    /// Returns the data stored in this buffer as a slice.
    pub fn as_slice(&self) -> &[T] {
        self
    }

    /// Returns the data stored in this buffer as a mutable slice.
    pub fn as_slice_mut(&mut self) -> &mut [T] {
        self
    }

    /// Returns a raw pointer to this buffer's internal memory
    /// This pointer is guaranteed to be aligned along cache-lines.
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.ptr.as_ptr()
    }

    /// Returns a mutable raw pointer to this buffer's internal memory
    /// This pointer is guaranteed to be aligned along cache-lines.
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.ptr.as_ptr()
    }

    /// Extends this buffer from a slice of items that can be represented in bytes, increasing its capacity if needed.
    /// # Example
    /// ```
    /// # use arrow2::buffer::MutableBuffer;
    /// let mut buffer = MutableBuffer::new();
    /// buffer.extend_from_slice(&[2u32, 0]);
    /// assert_eq!(buffer.len(), 2)
    /// ```
    #[inline]
    pub fn extend_from_slice(&mut self, items: &[T]) {
        let additional = items.len();
        self.reserve(additional);
        unsafe {
            let dst = self.ptr.as_ptr().add(self.len);
            let src = items.as_ptr();
            std::ptr::copy_nonoverlapping(src, dst, additional)
        }
        self.len += additional;
    }

    /// Extends the buffer with a new item, increasing its capacity if needed.
    /// # Example
    /// ```
    /// # use arrow2::buffer::MutableBuffer;
    /// let mut buffer = MutableBuffer::new();
    /// buffer.push(256u32);
    /// assert_eq!(buffer.len(), 1)
    /// ```
    #[inline]
    pub fn push(&mut self, item: T) {
        self.reserve(1);
        unsafe {
            let dst = self.ptr.as_ptr().add(self.len) as *mut T;
            std::ptr::write(dst, item);
        }
        self.len += 1;
    }

    /// Extends the buffer with a new item, without checking for sufficient capacity
    /// Safety
    /// Caller must ensure that the capacity()-len()>=size_of<T>()
    #[inline]
    pub(crate) unsafe fn push_unchecked(&mut self, item: T) {
        let dst = self.ptr.as_ptr().add(self.len);
        std::ptr::write(dst, item);
        self.len += 1;
    }

    /// # Safety
    /// The caller must ensure that the buffer was properly initialized up to `len`.
    #[inline]
    pub(crate) unsafe fn set_len(&mut self, len: usize) {
        assert!(len <= self.capacity());
        self.len = len;
    }

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
    /// assert!(buffer.capacity() == 8);
    /// ```
    pub fn shrink_to_fit(&mut self) {
        let new_capacity = capacity_multiple_of_64::<T>(self.len);
        if new_capacity < self.capacity {
            // JUSTIFICATION
            //  Benefit
            //      necessity
            //  Soundness
            //      `self.ptr` is valid for `self.capacity`.
            let ptr = unsafe { alloc::reallocate(self.ptr, self.capacity, new_capacity) };

            self.ptr = ptr;
            self.capacity = new_capacity;
        }
    }
}

/// # Safety
/// `ptr` must be allocated for `old_capacity`.
#[inline]
unsafe fn reallocate<T: NativeType>(
    ptr: NonNull<T>,
    old_capacity: usize,
    new_capacity: usize,
) -> (NonNull<T>, usize) {
    let new_capacity = capacity_multiple_of_64::<T>(new_capacity);
    let new_capacity = std::cmp::max(new_capacity, old_capacity * 2);
    let ptr = alloc::reallocate(ptr, old_capacity, new_capacity);
    (ptr, new_capacity)
}

impl<A: NativeType> Extend<A> for MutableBuffer<A> {
    fn extend<T: IntoIterator<Item = A>>(&mut self, iter: T) {
        let iterator = iter.into_iter();
        self.extend_from_iter(iterator)
    }
}

impl<T: NativeType> MutableBuffer<T> {
    #[inline]
    fn extend_from_iter<I: Iterator<Item = T>>(&mut self, mut iterator: I) {
        let (lower, _) = iterator.size_hint();
        let additional = lower;
        self.reserve(additional);

        // this is necessary because of https://github.com/rust-lang/rust/issues/32155
        let mut len = SetLenOnDrop::new(&mut self.len);
        let mut dst = unsafe { self.ptr.as_ptr().add(len.local_len) as *mut T };
        let capacity = self.capacity;

        while len.local_len < capacity {
            if let Some(item) = iterator.next() {
                unsafe {
                    std::ptr::write(dst, item);
                    dst = dst.add(1);
                }
                len.local_len += 1;
            } else {
                break;
            }
        }
        drop(len);

        iterator.for_each(|item| self.push(item));
    }

    /// Extends `self` from a [`TrustedLen`] iterator.
    #[inline]
    pub fn extend_from_trusted_len_iter<I: TrustedLen<Item = T>>(&mut self, iterator: I) {
        unsafe { self.extend_from_trusted_len_iter_unchecked(iterator) }
    }

    /// Extends `self` from an iterator.
    /// # Safety
    /// This method assumes that the iterator's size is correct and is undefined behavior
    /// to use it on an iterator that reports an incorrect length.
    pub unsafe fn extend_from_trusted_len_iter_unchecked<I: Iterator<Item = T>>(
        &mut self,
        iterator: I,
    ) {
        let (_, upper) = iterator.size_hint();
        let upper = upper.expect("trusted_len_iter requires an upper limit");
        let len = upper;

        self.reserve(len);
        let mut dst = self.ptr.as_ptr().add(self.len);
        for item in iterator {
            // note how there is no reserve here (compared with `extend_from_iter`)
            std::ptr::write(dst, item);
            dst = dst.add(1);
        }
        assert_eq!(
            dst.offset_from(self.ptr.as_ptr().add(self.len)) as usize,
            upper,
            "Trusted iterator length was not accurately reported"
        );
        self.len += len;
    }

    /// Creates a [`MutableBuffer`] from an [`Iterator`] with a trusted (upper) length.
    /// Prefer this to `collect` whenever possible, as it is faster ~60% faster.
    /// # Example
    /// ```
    /// # use arrow2::buffer::MutableBuffer;
    /// let v = vec![1u32];
    /// let iter = v.iter().map(|x| x * 2);
    /// let buffer = unsafe { MutableBuffer::from_trusted_len_iter(iter) };
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
    /// # Example
    /// ```
    /// # use arrow2::buffer::MutableBuffer;
    /// let v = vec![1u32];
    /// let iter = v.iter().map(|x| x * 2);
    /// let buffer = unsafe { MutableBuffer::from_trusted_len_iter(iter) };
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
    pub unsafe fn from_trusted_len_iter_unchecked<I: Iterator<Item = T>>(iterator: I) -> Self {
        let mut buffer = MutableBuffer::new();
        buffer.extend_from_trusted_len_iter_unchecked(iterator);
        buffer
    }

    /// Creates a [`MutableBuffer`] from an [`Iterator`] with a [`TrustedLen`] iterator, or errors
    /// if any of the items of the iterator is an error.
    #[inline]
    pub fn try_from_trusted_len_iter<E, I: TrustedLen<Item = std::result::Result<T, E>>>(
        iterator: I,
    ) -> std::result::Result<Self, E> {
        unsafe { Self::try_from_trusted_len_iter_unchecked(iterator) }
    }

    /// Creates a [`MutableBuffer`] from an [`Iterator`] with a trusted (upper) length or errors
    /// if any of the items of the iterator is an error.
    /// Prefer this to `collect` whenever possible, as it is faster ~60% faster.
    /// The only difference between this and [`try_from_trusted_len_iter`] is that this works
    /// on any iterator, while `try_from_trusted_len_iter` requires the iterator to implement the trait
    /// [`TrustedLen`], which not every iterator currently implements due to limitations of the Rust compiler.
    /// # Safety
    /// This method assumes that the iterator's size is correct and is undefined behavior
    /// to use it on an iterator that reports an incorrect length.
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

        let mut dst = buffer.ptr.as_ptr();
        for item in iterator {
            std::ptr::write(dst, item?);
            dst = dst.add(1);
        }
        assert_eq!(
            dst.offset_from(buffer.ptr.as_ptr()) as usize,
            upper,
            "Trusted iterator length was not accurately reported"
        );
        buffer.len = len;
        Ok(buffer)
    }
}

impl<T: NativeType> FromIterator<T> for MutableBuffer<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut iterator = iter.into_iter();

        // first iteration, which will likely reserve sufficient space for the buffer.
        let mut buffer = match iterator.next() {
            None => MutableBuffer::new(),
            Some(element) => {
                let (lower, _) = iterator.size_hint();
                let mut buffer = MutableBuffer::with_capacity(lower.saturating_add(1));
                unsafe {
                    std::ptr::write(buffer.as_mut_ptr(), element);
                    buffer.len = 1;
                }
                buffer
            }
        };

        buffer.extend_from_iter(iterator);
        buffer
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
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len) }
    }
}

impl<T: NativeType> std::ops::DerefMut for MutableBuffer<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len) }
    }
}

impl<T: NativeType> Drop for MutableBuffer<T> {
    fn drop(&mut self) {
        unsafe { alloc::free_aligned(self.ptr, self.capacity) };
    }
}

struct SetLenOnDrop<'a> {
    len: &'a mut usize,
    local_len: usize,
}

impl<'a> SetLenOnDrop<'a> {
    #[inline]
    fn new(len: &'a mut usize) -> Self {
        SetLenOnDrop {
            local_len: *len,
            len,
        }
    }
}

impl Drop for SetLenOnDrop<'_> {
    #[inline]
    fn drop(&mut self) {
        *self.len = self.local_len;
    }
}

impl<T: NativeType, P: AsRef<[T]>> From<P> for MutableBuffer<T> {
    #[inline]
    fn from(slice: P) -> Self {
        MutableBuffer::from_trusted_len_iter(slice.as_ref().iter().copied())
    }
}

impl<T: NativeType> From<MutableBuffer<T>> for Buffer<T> {
    #[inline]
    fn from(buffer: MutableBuffer<T>) -> Self {
        Buffer::from_bytes(buffer.into())
    }
}

impl<T: NativeType> From<MutableBuffer<T>> for Bytes<T> {
    #[inline]
    fn from(buffer: MutableBuffer<T>) -> Self {
        let result = unsafe {
            Bytes::new(
                buffer.ptr,
                buffer.len,
                Deallocation::Native(buffer.capacity),
            )
        };
        // so that the memory region is not deallocated.
        std::mem::forget(buffer);
        result
    }
}

impl From<MutableBuffer<u64>> for MutableBuffer<u8> {
    #[inline]
    fn from(buffer: MutableBuffer<u64>) -> Self {
        let ratio = std::mem::size_of::<u64>() / std::mem::size_of::<u8>();

        let capacity = buffer.capacity * ratio;
        let len = buffer.len * ratio;
        let ptr = unsafe { NonNull::new_unchecked(buffer.ptr.as_ptr() as *mut u8) };
        // so that the memory region is not deallocated; ownership was transfered
        std::mem::forget(buffer);
        Self { ptr, len, capacity }
    }
}

impl MutableBuffer<u8> {
    #[inline]
    pub fn from_chunk_iter<I: TrustedLen<Item = u64>>(iter: I) -> Self {
        MutableBuffer::from_trusted_len_iter(iter).into()
    }

    /// # Safety
    /// This method assumes that the iterator's size is correct and is undefined behavior
    /// to use it on an iterator that reports an incorrect length.
    #[inline]
    pub unsafe fn from_chunk_iter_unchecked<I: Iterator<Item = u64>>(iter: I) -> Self {
        MutableBuffer::from_trusted_len_iter_unchecked(iter).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default() {
        let b = MutableBuffer::<i32>::default();
        assert_eq!(b.len(), 0);
        assert_eq!(b.is_empty(), true);
    }

    #[test]
    fn with_capacity() {
        let b = MutableBuffer::<i32>::with_capacity(6);
        assert!(b.capacity() >= 6);
        assert_eq!(b.is_empty(), true);
    }

    #[test]
    fn from_len_zeroed() {
        let b = MutableBuffer::<i32>::from_len_zeroed(3);
        assert_eq!(b.len(), 3);
        assert_eq!(b.is_empty(), false);
        assert_eq!(b.as_slice(), &[0, 0, 0]);
    }

    #[test]
    fn resize() {
        let mut b = MutableBuffer::<i32>::new();
        b.resize(3, 1);
        assert_eq!(b.len(), 3);
        assert_eq!(b.as_slice(), &[1, 1, 1]);
        assert_eq!(b.as_slice_mut(), &[1, 1, 1]);
    }

    // branch that uses alloc_zeroed
    #[test]
    fn resize_from_zero() {
        let mut b = MutableBuffer::<i32>::new();
        b.resize(3, 0);
        assert_eq!(b.len(), 3);
        assert_eq!(b.as_slice(), &[0, 0, 0]);
    }

    #[test]
    fn resize_smaller() {
        let mut b = MutableBuffer::<i32>::from_len_zeroed(3);
        b.resize(2, 1);
        assert_eq!(b.len(), 2);
        assert_eq!(b.as_slice(), &[0, 0]);
    }

    #[test]
    fn extend_from_slice() {
        let mut b = MutableBuffer::<i32>::from_len_zeroed(1);
        b.extend_from_slice(&[1, 2]);
        assert_eq!(b.len(), 3);
        assert_eq!(b.as_slice(), &[0, 1, 2]);

        assert_eq!(unsafe { *b.as_ptr() }, 0);
        assert_eq!(unsafe { *b.as_mut_ptr() }, 0);
    }

    #[test]
    fn push() {
        let mut b = MutableBuffer::<i32>::new();
        for _ in 0..17 {
            b.push(1);
        }
        assert_eq!(b.len(), 17);
    }

    #[test]
    fn capacity() {
        let b = MutableBuffer::<f32>::with_capacity(10);
        assert_eq!(b.capacity(), 64 / std::mem::size_of::<f32>());
        let b = MutableBuffer::<f32>::with_capacity(16);
        assert_eq!(b.capacity(), 16);

        let b = MutableBuffer::<f32>::with_capacity(64);
        assert!(b.capacity() >= 64);

        let mut b = MutableBuffer::<f32>::with_capacity(16);
        b.reserve(4);
        assert_eq!(b.capacity(), 16);
        b.extend_from_slice(&[0.1; 16]);
        b.reserve(4);
        assert_eq!(b.capacity(), 32);
    }

    #[test]
    fn extend() {
        let mut b = MutableBuffer::<i32>::new();
        b.extend(0..3);
        assert_eq!(b.as_slice(), &[0, 1, 2]);
    }

    #[test]
    fn extend_constant() {
        let mut b = MutableBuffer::<i32>::new();
        b.extend_constant(3, 1);
        assert_eq!(b.as_slice(), &[1, 1, 1]);
    }

    #[test]
    fn from_iter() {
        let b = (0..3).collect::<MutableBuffer<i32>>();
        assert_eq!(b.as_slice(), &[0, 1, 2]);
    }

    #[test]
    fn from_as_ref() {
        let b = MutableBuffer::<i32>::from(&[0, 1, 2]);
        assert_eq!(b.as_slice(), &[0, 1, 2]);
    }

    #[test]
    fn from_trusted_len_iter() {
        let b = unsafe { MutableBuffer::<i32>::from_trusted_len_iter_unchecked(0..3) };
        assert_eq!(b.as_slice(), &[0, 1, 2]);
    }

    #[test]
    fn try_from_trusted_len_iter() {
        let iter = (0..3).map(Result::<_, String>::Ok);
        let buffer =
            unsafe { MutableBuffer::<i32>::try_from_trusted_len_iter_unchecked(iter) }.unwrap();
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.as_slice(), &[0, 1, 2]);
    }

    #[test]
    fn to_buffer() {
        let b = (0..3).collect::<MutableBuffer<i32>>();
        let b: Buffer<i32> = b.into();
        assert_eq!(b.as_slice(), &[0, 1, 2]);
    }

    #[test]
    fn to_bytes() {
        let b = (0..3).collect::<MutableBuffer<i32>>();
        let b: Bytes<i32> = b.into();
        assert_eq!(b.as_ref(), &[0, 1, 2]);
    }
}
