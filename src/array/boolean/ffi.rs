use crate::{
    array::{FromFfi, ToFfi},
    ffi,
};

use crate::error::Result;

use super::BooleanArray;

unsafe impl ToFfi for BooleanArray {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        vec![
            self.validity.as_ref().map(|x| x.as_ptr()),
            Some(self.values.as_ptr()),
        ]
    }

    fn offset(&self) -> usize {
        self.offset
    }
}

unsafe impl<A: ffi::ArrowArrayRef> FromFfi<A> for BooleanArray {
    fn try_from_ffi(array: A) -> Result<Self> {
        let length = array.array().len();
        let offset = array.array().offset();
        let mut validity = unsafe { array.validity() }?;
        let mut values = unsafe { array.bitmap(0) }?;

        if offset > 0 {
            values = values.slice(offset, length);
            validity = validity.map(|x| x.slice(offset, length))
        }
        Ok(Self::from_data(values, validity))
    }
}
