use crate::{datatypes::DataType, ffi::ArrowArray};

use crate::error::Result;

pub unsafe trait ToFFI {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3];

    fn offset(&self) -> usize;
}

pub unsafe trait FromFFI: Sized {
    fn try_from_ffi(data_type: DataType, array: ArrowArray) -> Result<Self>;
}
