//! This module contains an implementation of a contiguous immutable memory region that knows
//! how to de-allocate itself, [`Bytes`].

use std::mem::ManuallyDrop;
use std::{fmt::Debug, fmt::Formatter};
use std::{ptr::NonNull, sync::Arc};

use crate::ffi;
use crate::types::NativeType;

/// Mode of deallocating memory regions
pub enum Deallocation {
    /// Native deallocation, using Rust deallocator with Arrow-specific memory aligment
    Native,
    // Foreign interface, via a callback
    Foreign(Arc<ffi::ArrowArray>),
}

impl Debug for Deallocation {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Deallocation::Native => {
                write!(f, "Deallocation::Native")
            }
            Deallocation::Foreign(_) => {
                write!(f, "Deallocation::Foreign {{ capacity: unknown }}")
            }
        }
    }
}

/// A continuous, fixed-size, immutable memory region that knows how to de-allocate itself.
///
/// In the most common case, this buffer is allocated using [`allocate_aligned`](alloc::allocate_aligned)
/// and deallocated accordingly [`free_aligned`](alloc::free_aligned).
/// When the region is allocated by a foreign allocator, [Deallocation::Foreign], this calls the
/// foreign deallocator to deallocate the region when it is no longer needed.
pub struct Bytes<T: NativeType> {
    /// inner data
    data: Vec<T>,
    /// how to deallocate this region
    deallocation: Deallocation,
}

impl<T: NativeType> Bytes<T> {
    /// Takes ownership of an allocated memory region,
    ///
    /// # Arguments
    ///
    /// * `ptr` - Pointer to raw parts
    /// * `len` - Length of raw parts in **bytes**
    /// * `capacity` - Total allocated memory for the pointer `ptr`, in **bytes**
    ///
    /// # Safety
    ///
    /// This function is unsafe as there is no guarantee that the given pointer is valid for `len`
    /// bytes. If the `ptr` and `capacity` come from a `Buffer`, then this is guaranteed.
    ///
    /// # Panics
    ///
    /// This function panics if the give deallocation not is `Deallocation::Foreign`
    #[inline]
    pub unsafe fn from_ffi(
        ptr: std::ptr::NonNull<T>,
        len: usize,
        deallocation: Deallocation,
    ) -> Self {
        let data = Vec::from_raw_parts(ptr.as_ptr(), len, len);
        assert!(matches!(deallocation, Deallocation::Foreign(_)));

        Self { data, deallocation }
    }

    #[inline]
    fn as_slice(&self) -> &[T] {
        self
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn ptr(&self) -> NonNull<T> {
        debug_assert!(!self.data.as_ptr().is_null());
        unsafe { NonNull::new_unchecked(self.data.as_ptr() as *mut T) }
    }
}

impl<T: NativeType> Drop for Bytes<T> {
    #[inline]
    fn drop(&mut self) {
        match &self.deallocation {
            // the Vec may be dropped
            Deallocation::Native => {}
            // foreign interface knows how to deallocate itself.
            Deallocation::Foreign(_) => {
                let data = std::mem::take(&mut self.data);
                let _ = ManuallyDrop::new(data);
            }
        }
    }
}

impl<T: NativeType> std::ops::Deref for Bytes<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        &self.data
    }
}

impl<T: NativeType> PartialEq for Bytes<T> {
    fn eq(&self, other: &Bytes<T>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T: NativeType> Debug for Bytes<T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Bytes {{ ptr: {:?}, len: {}, data: ",
            self.data.as_ptr(),
            self.len(),
        )?;

        f.debug_list().entries(self.iter()).finish()?;

        write!(f, " }}")
    }
}

impl<T: NativeType> From<Vec<T>> for Bytes<T> {
    #[inline]
    fn from(data: Vec<T>) -> Self {
        Self {
            data,
            deallocation: Deallocation::Native,
        }
    }
}

// This is sound because `Bytes` is an immutable container
unsafe impl<T: NativeType> Send for Bytes<T> {}
unsafe impl<T: NativeType> Sync for Bytes<T> {}
