// this code is in its own module so that inner types are not accessible
// as that might break invariants assumptions
use crate::types::NativeType;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};

/// Holds a `Vec` that may hold a pointer that is not allocated by `Vec`. It is therefore not
/// safe to deallocate the inner type naively
///
/// This struct exists to avoid holding an `enum` of a `Vec` or a foreign pointer, whose `deref`
/// is known to be least 50% more expensive than the deref of a `Vec`.
///
/// # Safety
///
/// it is unsafe to take and drop the inner value of `MaybeForeign`
/// Doing so is only allowed if the `Vec` was created from the native `Vec` allocator
pub(super) struct MaybeForeign<T: NativeType> {
    inner: ManuallyDrop<Vec<T>>,
}

impl<T: NativeType> MaybeForeign<T> {
    #[inline]
    pub(super) fn new(data: Vec<T>) -> Self {
        Self {
            inner: ManuallyDrop::new(data),
        }
    }

    /// # Safety
    /// This function may only be called if the inner `Vec<T>` was allocated
    /// by `Vec<T, A>` allocator `A`.
    #[inline]
    pub(super) unsafe fn drop_local(&mut self) {
        let data = std::mem::take(&mut self.inner);
        let _data = ManuallyDrop::into_inner(data);
    }

    /// # Safety
    /// This function may only be called if the inner `Vec<T>` was allocated
    /// in Rust and the default `Vec<T, A>` allocator `A`.
    /// Otherwise, users may reallocate `Vec`, which is unsound
    #[inline]
    pub(super) unsafe fn mut_vec(&mut self) -> &mut Vec<T> {
        self.inner.deref_mut()
    }
}

impl<T: NativeType> Deref for MaybeForeign<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
