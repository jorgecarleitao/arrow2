use std::iter::FromIterator;
use std::ptr::NonNull;

use crate::alloc;
use crate::types::NativeType;

/// Returns the nearest number that is `>=` than `num` and is a multiple of 64
#[inline]
fn round_upto_multiple_of_64(num: usize) -> usize {
    round_upto_power_of_2(num, 64)
}

/// Returns the nearest multiple of `factor` that is `>=` than `num`. Here `factor` must
/// be a power of 2.
fn round_upto_power_of_2(num: usize, factor: usize) -> usize {
    debug_assert!(factor > 0 && (factor & (factor - 1)) == 0);
    (num + (factor - 1)) & !(factor - 1)
}

#[inline]
fn capacity_multiple_of_64<T: NativeType>(capacity: usize) -> usize {
    round_upto_multiple_of_64(capacity * std::mem::size_of::<T>()) / std::mem::size_of::<T>()
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

/// An interface equivalent to `std::vec::Vec` with an allocator aligned along cache-lines.
pub struct AlignedVec<T: NativeType> {
    // dangling iff capacity = 0
    ptr: NonNull<T>,
    // invariant: len <= capacity
    len: usize,
    capacity: usize,
}

impl<T: NativeType> AlignedVec<T> {
    #[inline]
    pub fn new() -> Self {
        let ptr = alloc::allocate_aligned(0);
        Self {
            ptr,
            len: 0,
            capacity: 0,
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.len = 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    pub fn truncate(&mut self, len: usize) {
        if len < self.len {
            self.len = len;
        }
    }

    /// Sets the length of this buffer.
    /// # Safety:
    /// The caller must uphold the following invariants:
    /// * ensure no reads are performed on any
    ///     item within `[len, capacity - len]`
    /// * ensure `len <= self.capacity()`
    #[inline]
    pub unsafe fn set_len(&mut self, len: usize) {
        debug_assert!(len <= self.capacity());
        self.len = len;
    }

    /// Returns the data stored in this buffer as a slice.
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        self
    }

    /// Returns the data stored in this buffer as a mutable slice.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
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

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

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

    #[inline]
    pub fn push(&mut self, item: T) {
        self.reserve(1);
        unsafe {
            let dst = self.ptr.as_ptr().add(self.len) as *mut T;
            std::ptr::write(dst, item);
        }
        self.len += 1;
    }

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

    #[inline]
    pub unsafe fn from_raw_parts(ptr: NonNull<T>, length: usize, capacity: usize) -> Self {
        Self {
            ptr,
            capacity,
            len: length,
        }
    }
}

impl<T: NativeType> Default for AlignedVec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: NativeType> std::ops::Deref for AlignedVec<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len) }
    }
}

impl<T: NativeType> std::ops::DerefMut for AlignedVec<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len) }
    }
}

impl<A: NativeType> Extend<A> for AlignedVec<A> {
    fn extend<T: IntoIterator<Item = A>>(&mut self, iter: T) {
        let mut iterator = iter.into_iter();
        let (lower, _) = iterator.size_hint();
        let additional = lower;
        self.reserve(additional);

        // this is necessary because of https://github.com/rust-lang/rust/issues/32155
        let mut len = SetLenOnDrop::new(&mut self.len);
        let mut dst = unsafe { self.ptr.as_ptr().add(len.local_len) as *mut A };
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

impl<T: NativeType> FromIterator<T> for AlignedVec<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut iterator = iter.into_iter();

        // first iteration, which will likely reserve sufficient space for the buffer.
        let mut buffer = match iterator.next() {
            None => AlignedVec::new(),
            Some(element) => {
                let (lower, _) = iterator.size_hint();
                let mut buffer = AlignedVec::with_capacity(lower.saturating_add(1));
                unsafe {
                    std::ptr::write(buffer.as_mut_ptr(), element);
                    buffer.len = 1;
                }
                buffer
            }
        };

        buffer.extend(iterator);
        buffer
    }
}
