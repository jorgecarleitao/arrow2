use crate::error::Result;
use crate::{
    array::{FromFfi, Offset, ToFfi},
    datatypes::DataType,
    ffi::ArrowArray,
};

use super::Utf8Array;

unsafe impl<O: Offset> ToFfi for Utf8Array<O> {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3] {
        unsafe {
            [
                self.validity.as_ref().map(|x| x.as_ptr()),
                Some(std::ptr::NonNull::new_unchecked(
                    self.offsets.as_ptr() as *mut u8
                )),
                Some(std::ptr::NonNull::new_unchecked(
                    self.values.as_ptr() as *mut u8
                )),
            ]
        }
    }

    fn offset(&self) -> usize {
        self.offset
    }
}

unsafe impl<O: Offset> FromFfi for Utf8Array<O> {
    fn try_from_ffi(_: DataType, array: ArrowArray) -> Result<Self> {
        let length = array.len();
        let offset = array.offset();
        let mut validity = array.validity();
        let mut offsets = unsafe { array.buffer::<O>(0)? };
        let values = unsafe { array.buffer::<u8>(1)? };

        if offset > 0 {
            offsets = offsets.slice(offset, length);
            validity = validity.map(|x| x.slice(offset, length))
        }
        Ok(Self::from_data(offsets, values, validity))
    }
}
