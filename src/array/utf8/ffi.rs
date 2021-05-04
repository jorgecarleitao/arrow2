use crate::{
    array::{FromFfi, Offset, ToFfi},
    error::Result,
    ffi,
};

use super::Utf8Array;

unsafe impl<O: Offset> ToFfi for Utf8Array<O> {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        unsafe {
            vec![
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

unsafe impl<O: Offset, A: ffi::ArrowArrayRef> FromFfi<A> for Utf8Array<O> {
    fn try_from_ffi(array: A) -> Result<Self> {
        let length = array.array().len();
        let offset = array.array().offset();
        let mut validity = unsafe { array.validity() }?;
        let mut offsets = unsafe { array.buffer::<O>(0) }?;
        let values = unsafe { array.buffer::<u8>(1)? };

        if offset > 0 {
            offsets = offsets.slice(offset, length);
            validity = validity.map(|x| x.slice(offset, length))
        }
        Ok(Self::from_data(offsets, values, validity))
    }
}
