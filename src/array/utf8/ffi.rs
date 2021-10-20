use crate::{
    array::{FromFfi, Offset, ToFfi},
    error::Result,
    ffi,
};

use super::Utf8Array;

unsafe impl<O: Offset> ToFfi for Utf8Array<O> {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        let offset = self
            .validity
            .as_ref()
            .map(|x| x.offset())
            .unwrap_or_default();

        let offsets = std::ptr::NonNull::new(unsafe {
            self.offsets.as_ptr().offset(-(offset as isize)) as *mut u8
        });

        vec![
            self.validity.as_ref().map(|x| x.as_ptr()),
            offsets,
            std::ptr::NonNull::new(self.values.as_ptr() as *mut u8),
        ]
    }
}

impl<O: Offset, A: ffi::ArrowArrayRef> FromFfi<A> for Utf8Array<O> {
    unsafe fn try_from_ffi(array: A) -> Result<Self> {
        let validity = unsafe { array.validity() }?;
        let offsets = unsafe { array.buffer::<O>(0) }?;
        let values = unsafe { array.buffer::<u8>(1)? };

        let data_type = Self::default_data_type();
        Ok(Self::from_data_unchecked(
            data_type, offsets, values, validity,
        ))
    }
}
