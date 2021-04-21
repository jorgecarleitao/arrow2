use crate::{
    array::{FromFfi, ToFfi},
    datatypes::DataType,
    ffi::ArrowArray,
};

use crate::error::Result;

use super::BooleanArray;

unsafe impl ToFfi for BooleanArray {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3] {
        [
            self.validity.as_ref().map(|x| x.as_ptr()),
            Some(self.values.as_ptr()),
            None,
        ]
    }

    fn offset(&self) -> usize {
        self.offset
    }
}

unsafe impl FromFfi for BooleanArray {
    fn try_from_ffi(_: DataType, array: ArrowArray) -> Result<Self> {
        let length = array.len();
        let offset = array.offset();
        let mut validity = array.validity();
        let mut values = unsafe { array.bitmap(0)? };

        if offset > 0 {
            values = values.slice(offset, length);
            validity = validity.map(|x| x.slice(offset, length))
        }
        Ok(Self::from_data(values, validity))
    }
}
