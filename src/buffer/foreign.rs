use crate::types::NativeType;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
// this code is in its own module so that inner types are not accessible
// as that might break invariants assumptions

/// Holds a `Vec` that may hold a pointer memory that is not
/// allocated by `Vec`. It is therefore not
/// safe to deallocate the inner type naively
///
/// # Safety
///
/// it is unsafe to take and drop the inner value of `MaybeForeign`
/// Doing so is only allowed if the `Vec` was created from the native `Vec` allocator
pub(super) struct MaybeForeign<T: NativeType> {
    inner: ManuallyDrop<Vec<T>>,
}

impl<T: NativeType> MaybeForeign<T> {
    pub(super) fn new(data: Vec<T>) -> Self {
        Self {
            inner: ManuallyDrop::new(data),
        }
    }

    /// # Safety
    /// This function may only be called if the inner `Vec<T>` was allocated
    /// in Rust and the default `Vec<T, A>` allocator `A`.
    pub(super) unsafe fn drop_local(&mut self) {
        let data = std::mem::take(&mut self.inner);
        let _data = ManuallyDrop::into_inner(data);
    }

    /// # Safety
    /// This function may only be called if the inner `Vec<T>` was allocated
    /// in Rust and the default `Vec<T, A>` allocator `A`.
    pub(super) unsafe fn mut_vec(&mut self) -> &mut Vec<T> {
        self.inner.deref_mut()
    }
}

impl<T: NativeType> Deref for MaybeForeign<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
