use std::sync::Arc;

use crate::{array::Array, ffi};

use crate::error::Result;

/// Trait describing how a struct presents itself to the
/// [C data interface](https://arrow.apache.org/docs/format/CDataInterface.html) (FFI).
pub unsafe trait ToFfi {
    /// The pointers to the buffers.
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>>;

    /// The offset
    fn offset(&self) -> usize;

    /// The children
    fn children(&self) -> Vec<Arc<dyn Array>> {
        vec![]
    }
}

/// Trait describing how a struct imports into itself from the
/// [C data interface](https://arrow.apache.org/docs/format/CDataInterface.html) (FFI).
pub unsafe trait FromFfi<T: ffi::ArrowArrayRef>: Sized {
    fn try_from_ffi(array: T) -> Result<Self>;
}
