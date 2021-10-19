use crate::{
    array::{FromFfi, ToFfi},
    ffi,
    types::NativeType,
};

use crate::error::Result;

use super::PrimitiveArray;

unsafe impl<T: NativeType> ToFfi for PrimitiveArray<T> {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        let offset = self
            .validity
            .as_ref()
            .map(|x| x.offset())
            .unwrap_or_default() as isize;
        vec![
            self.validity.as_ref().map(|x| x.as_ptr()),
            std::ptr::NonNull::new(unsafe { self.values.as_ptr().offset(-offset) as *mut u8 }),
        ]
    }
}

impl<T: NativeType, A: ffi::ArrowArrayRef> FromFfi<A> for PrimitiveArray<T> {
    unsafe fn try_from_ffi(array: A) -> Result<Self> {
        let data_type = array.field().data_type().clone();
        let validity = unsafe { array.validity() }?;
        let values = unsafe { array.buffer::<T>(0) }?;

        Ok(Self::from_data(data_type, values, validity))
    }
}
