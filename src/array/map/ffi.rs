use std::sync::Arc;

use crate::{array::FromFfi, error::Result, ffi};

use super::super::{ffi::ToFfi, Array};
use super::MapArray;

unsafe impl ToFfi for MapArray {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        vec![
            self.validity.as_ref().map(|x| x.as_ptr()),
            std::ptr::NonNull::new(self.offsets.as_ptr() as *mut u8),
        ]
    }

    fn offset(&self) -> usize {
        self.offset
    }

    fn children(&self) -> Vec<Arc<dyn Array>> {
        vec![self.field.clone()]
    }
}

impl<A: ffi::ArrowArrayRef> FromFfi<A> for MapArray {
    unsafe fn try_from_ffi(array: A) -> Result<Self> {
        let data_type = array.field().data_type().clone();
        let validity = unsafe { array.validity() }?;
        let offsets = unsafe { array.buffer::<i32>(0) }?;
        let child = array.child(0)?;
        let values = ffi::try_from(child)?.into();

        Ok(Self::from_data(data_type, offsets, values, validity))
    }
}
