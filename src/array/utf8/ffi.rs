use crate::{
    array::{FromFfi, Offset, ToFfi},
    error::Result,
    ffi,
};

use super::Utf8Array;

unsafe impl<O: Offset> ToFfi for Utf8Array<O> {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        vec![
            self.validity.as_ref().map(|x| x.as_ptr()),
            std::ptr::NonNull::new(self.offsets.as_ptr() as *mut u8),
            std::ptr::NonNull::new(self.values.as_ptr() as *mut u8),
        ]
    }

    fn offset(&self) -> usize {
        self.offset
    }
}

impl<O: Offset, A: ffi::ArrowArrayRef> FromFfi<A> for Utf8Array<O> {
    unsafe fn try_from_ffi(array: A) -> Result<Self> {
        let length = array.array().len();
        let offset = array.array().offset();
        let mut validity = unsafe { array.validity() }?;
        let mut offsets = unsafe { array.buffer::<O>(0) }?;
        let values = unsafe { array.buffer::<u8>(1)? };

        if offset > 0 {
            offsets = offsets.slice(offset, length);
            validity = validity.map(|x| x.slice(offset, length))
        }
        let data_type = Self::default_data_type();
        Ok(Self::from_data(data_type, offsets, values, validity))
    }
}
