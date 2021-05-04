use crate::{
    array::{FromFfi, ToFfi},
    ffi,
    types::NativeType,
};

use crate::error::Result;

use super::PrimitiveArray;

unsafe impl<T: NativeType> ToFfi for PrimitiveArray<T> {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        unsafe {
            vec![
                self.validity.as_ref().map(|x| x.as_ptr()),
                Some(std::ptr::NonNull::new_unchecked(
                    self.values.as_ptr() as *mut u8
                )),
            ]
        }
    }

    #[inline]
    fn offset(&self) -> usize {
        self.offset
    }
}

unsafe impl<T: NativeType, A: ffi::ArrowArrayRef> FromFfi<A> for PrimitiveArray<T> {
    fn try_from_ffi(array: A) -> Result<Self> {
        let data_type = array.data_type()?;
        let length = array.array().len();
        let offset = array.array().offset();
        let mut validity = unsafe { array.validity() }?;
        let mut values = unsafe { array.buffer::<T>(0) }?;

        if offset > 0 {
            values = values.slice(offset, length);
            validity = validity.map(|x| x.slice(offset, length))
        }
        Ok(Self::from_data(data_type, values, validity))
    }
}
