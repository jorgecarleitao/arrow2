use std::sync::Arc;

use crate::{array::Array, ffi};

use crate::error::Result;

/// Trait describing how a struct presents itself when converting itself to the C data interface (FFI).
pub unsafe trait ToFfi {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>>;

    fn offset(&self) -> usize;

    fn children(&self) -> Vec<Arc<dyn Array>> {
        vec![]
    }
}

/// Trait describing how a creates itself from the C data interface (FFI).
pub unsafe trait FromFfi<T: ffi::ArrowArrayRef>: Sized {
    fn try_from_ffi(array: T) -> Result<Self>;
}
