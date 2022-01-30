//! This module contains an implementation of a contiguous immutable memory region that knows
//! how to de-allocate itself, [`Bytes`].

use std::mem::ManuallyDrop;
use std::{fmt::Debug, fmt::Formatter};
use std::{ptr::NonNull, sync::Arc};

use crate::ffi;
use crate::types::NativeType;

/// Holds a `Vec` that may hold a pointer memory that is not
/// allocated by `Vec`. It is therefore not
/// safe to deallocate the inner type naively
struct MaybeUnaligned<T: NativeType> {
    inner: Vec<T>,
    aligned: bool,
}

impl<T: NativeType> Drop for MaybeUnaligned<T> {
    fn drop(&mut self) {
        // this memory is not allocated by `Vec`, we leak it
        // this memory will be deallocated by the foreign interface
        if !self.aligned {
            let data = std::mem::take(&mut self.inner);
            let _ = ManuallyDrop::new(data);
        }
    }
}

impl<T: NativeType> MaybeUnaligned<T> {
    /// # Safety
    /// The caller must ensure that if `aligned` the `Vec<T>` holds
    /// all invariants set by https://doc.rust-lang.org/std/vec/struct.Vec.html#safety
    unsafe fn new(data: Vec<T>, aligned: bool) -> Self {
        Self {
            inner: data,
            aligned,
        }
    }
}

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
    data: MaybeUnaligned<T>,
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
        let data = Vec::from_raw_parts(ptr.as_ptr(), len, len);
        let data = MaybeUnaligned::new(data, false);

        Self { data, deallocation }
    }

    #[inline]
    fn as_slice(&self) -> &[T] {
        self
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.inner.len()
    }

    #[inline]
    pub fn ptr(&self) -> NonNull<T> {
        debug_assert!(!self.data.inner.as_ptr().is_null());
        unsafe { NonNull::new_unchecked(self.data.inner.as_ptr() as *mut T) }
    }

    /// Returns a mutable reference to the `Vec<T>` data if it is allocated in this process.
    /// Returns `None` if allocated by a foreign interface.
    ///
    /// See also [`Bytes::make_vec`], which will clone the inner data when allocated via ffi.
    pub fn get_vec(&mut self) -> Option<&mut Vec<T>> {
        match &self.deallocation {
            Deallocation::Foreign(_) => None,
            Deallocation::Native => Some(&mut self.data.inner),
        }
    }

    /// Returns a mutable reference to the `Vec<T>` data if it is allocated in this process.
    /// This function will clone the inner data when allocated via ffi.
    ///
    /// See also [`Bytes::get_vec`], which will return `None` if this [`Bytes`] does not own its data.
    pub fn make_vec(&mut self) -> &mut Vec<T> {
        match &self.deallocation {
            // We clone the data, set that as native allocation
            // and return a mutable reference to the new data
            Deallocation::Foreign(_) => {
                self.deallocation = Deallocation::Native;
                let new_data = self.data.inner.clone();
                // Safety:
                // the new data is allocated by Vec itself
                let data = unsafe { MaybeUnaligned::new(new_data, true) };

                self.data = data;
                &mut self.data.inner
            }
            Deallocation::Native => &mut self.data.inner,
        }
    }
}

impl<T: NativeType> std::ops::Deref for Bytes<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        &self.data.inner
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
            self.data.inner.as_ptr(),
            self.len(),
        )?;

        f.debug_list().entries(self.iter()).finish()?;

        write!(f, " }}")
    }
}

impl<T: NativeType> From<Vec<T>> for Bytes<T> {
    #[inline]
    fn from(data: Vec<T>) -> Self {
        // Safety:
        // A `Vec` is allocated by `Vec`
        let data = unsafe { MaybeUnaligned::new(data, true) };
        Self {
            data,
            deallocation: Deallocation::Native,
        }
    }
}

// This is sound because `Bytes` is an immutable container
unsafe impl<T: NativeType> Send for Bytes<T> {}
unsafe impl<T: NativeType> Sync for Bytes<T> {}
