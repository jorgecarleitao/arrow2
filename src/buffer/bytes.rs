//! This module contains an implementation of a contiguous immutable memory region that knows
//! how to de-allocate itself, [`Bytes`].

use std::{fmt::Debug, fmt::Formatter};
use std::{ptr::NonNull, sync::Arc};

use super::foreign::MaybeForeign;
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
    data: MaybeForeign<T>,
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
        assert!(matches!(deallocation, Deallocation::Foreign(_)));
        // This line is technically outside the assumptions of `Vec::from_raw_parts`, since
        // `ptr` was not allocated by `Vec`. However, one of the invariants of this struct
        // is that we do not expose this region as a `Vec`; we only use `Vec` on it to provide
        // immutable access to the region (via `Vec::deref` to `&[T]`).
        let data = Vec::from_raw_parts(ptr.as_ptr(), len, len);
        let data = MaybeForeign::new(data);

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

    /// Returns a mutable reference to the internal [`Vec<T>`] if it is natively allocated.
    /// Returns `None` if allocated by a foreign interface.
    pub fn get_vec(&mut self) -> Option<&mut Vec<T>> {
        match &self.deallocation {
            Deallocation::Foreign(_) => None,
            // Safety:
            // The allocation is native so we can share the vec
            Deallocation::Native => Some(unsafe { self.data.mut_vec() }),
        }
    }
}

impl<T: NativeType> Drop for Bytes<T> {
    fn drop(&mut self) {
        match self.deallocation {
            // a foreign interface knows how to deallocate itself
            Deallocation::Foreign(_) => {}
            Deallocation::Native => {
                // Safety:
                // the allocation is native, so we can safely drop
                unsafe { self.data.drop_local() }
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
        let data = MaybeForeign::new(data);
        Self {
            data,
            deallocation: Deallocation::Native,
        }
    }
}

// This is sound because `Bytes` is an immutable container
unsafe impl<T: NativeType> Send for Bytes<T> {}
unsafe impl<T: NativeType> Sync for Bytes<T> {}
