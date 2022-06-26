use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::panic::RefUnwindSafe;
use std::ptr::NonNull;

/// Mode of deallocating memory regions
enum Allocation {
    /// Native allocation
    Native,
    // A foreign allocator and its ref count
    Foreign(Box<dyn RefUnwindSafe + Send + Sync>),
}

/// A continuous memory region that may be allocated externally.
///
/// In the most common case, this is created from [`Vec`].
/// However, this region can also be allocated by a foreign allocator.
pub struct Bytes<T> {
    /// An implementation using an `enum` of a `Vec` or a foreign pointer is not used
    /// because `deref` is at least 50% more expensive than the deref of a `Vec`.
    data: ManuallyDrop<Vec<T>>,
    /// the region was allocated
    allocation: Allocation,
}

impl<T> Bytes<T> {
    /// Takes ownership of an allocated memory region `[ptr, ptr+len[`,
    /// # Safety
    /// This function is safe iff:
    /// * the region is properly allocated in that a slice can be safely built from it.
    /// * the region is immutable.
    /// # Implementation
    /// This function leaks iff `owner` does not deallocate the region when dropped.
    #[inline]
    pub unsafe fn from_owned(
        ptr: std::ptr::NonNull<T>,
        len: usize,
        owner: Box<dyn RefUnwindSafe + Send + Sync>,
    ) -> Self {
        // This line is technically outside the assumptions of `Vec::from_raw_parts`, since
        // `ptr` was not allocated by `Vec`. However, one of the invariants of this struct
        // is that we do not expose this region as a `Vec`; we only use `Vec` on it to provide
        // immutable access to the region (via `Vec::deref` to `&[T]`).
        // MIRI does not complain, which seems to agree with the line of thought.
        let data = Vec::from_raw_parts(ptr.as_ptr(), len, len);
        let data = ManuallyDrop::new(data);

        Self {
            data,
            allocation: Allocation::Foreign(owner),
        }
    }

    /// The length of the region
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// The pointer to the region
    #[inline]
    pub fn ptr(&self) -> NonNull<T> {
        unsafe { NonNull::new_unchecked(self.data.as_ptr() as *mut T) }
    }

    /// Returns a `Some` mutable reference of [`Vec<T>`] iff this was initialized
    /// from a [`Vec<T>`] and `None` otherwise.
    #[inline]
    pub fn get_vec(&mut self) -> Option<&mut Vec<T>> {
        match &self.allocation {
            Allocation::Foreign(_) => None,
            Allocation::Native => Some(self.data.deref_mut()),
        }
    }
}

impl<T> Drop for Bytes<T> {
    #[inline]
    fn drop(&mut self) {
        match self.allocation {
            Allocation::Foreign(_) => {
                // The ref count of the foreign is reduced by one
                // we can't deallocate `Vec` here since the region was allocated by
                // a foreign allocator
            }
            Allocation::Native => {
                let data = std::mem::take(&mut self.data);
                let _ = ManuallyDrop::into_inner(data);
            }
        }
    }
}

impl<T> std::ops::Deref for Bytes<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        &self.data
    }
}

impl<T: PartialEq> PartialEq for Bytes<T> {
    #[inline]
    fn eq(&self, other: &Bytes<T>) -> bool {
        self.deref() == other.deref()
    }
}

impl<T> From<Vec<T>> for Bytes<T> {
    #[inline]
    fn from(data: Vec<T>) -> Self {
        Self {
            data: ManuallyDrop::new(data),
            allocation: Allocation::Native,
        }
    }
}
