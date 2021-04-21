use crate::{datatypes::DataType, ffi::ArrowArray};

use crate::error::Result;

/// Trait describing how a struct presents itself when converting itself to the C data interface (FFI).
pub unsafe trait ToFfi {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3];

    fn offset(&self) -> usize;
}

/// Trait describing how a creates itself from the C data interface (FFI).
pub unsafe trait FromFfi: Sized {
    fn try_from_ffi(data_type: DataType, array: ArrowArray) -> Result<Self>;
}
