use crate::{
    array::{FromFFI, ToFFI},
    datatypes::DataType,
    ffi::ArrowArray,
    types::NativeType,
};

use crate::error::Result;

use super::PrimitiveArray;

unsafe impl<T: NativeType> ToFFI for PrimitiveArray<T> {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3] {
        unsafe {
            [
                self.validity.as_ref().map(|x| x.as_ptr()),
                Some(std::ptr::NonNull::new_unchecked(
                    self.values.as_ptr() as *mut u8
                )),
                None,
            ]
        }
    }

    #[inline]
    fn offset(&self) -> usize {
        self.offset
    }
}

unsafe impl<T: NativeType> FromFFI for PrimitiveArray<T> {
    fn try_from_ffi(data_type: DataType, array: ArrowArray) -> Result<Self> {
        let length = array.len();
        let offset = array.offset();
        let mut validity = array.validity();
        let mut values = unsafe { array.buffer::<T>(0)? };

        if offset > 0 {
            values = values.slice(offset, length);
            validity = validity.map(|x| x.slice(offset, length))
        }
        Ok(Self::from_data(data_type, values, validity))
    }
}
