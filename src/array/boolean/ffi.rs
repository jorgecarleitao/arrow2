use crate::{
    array::{FromFfi, ToFfi},
    datatypes::DataType,
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

impl<A: ffi::ArrowArrayRef> FromFfi<A> for BooleanArray {
    unsafe fn try_from_ffi(array: A) -> Result<Self> {
        let data_type = array.field().data_type().clone();
        assert_eq!(data_type, DataType::Boolean);

        let validity = unsafe { array.validity() }?;
        let values = unsafe { array.bitmap(0) }?;

        Ok(Self::from_data(data_type, values, validity))
    }
}
