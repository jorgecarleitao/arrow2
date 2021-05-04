use std::sync::Arc;

use crate::{array::FromFfi, error::Result, ffi};

use super::super::{ffi::ToFfi, specification::Offset, Array};
use super::ListArray;

unsafe impl<O: Offset> ToFfi for ListArray<O> {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        unsafe {
            vec![
                self.validity.as_ref().map(|x| x.as_ptr()),
                Some(std::ptr::NonNull::new_unchecked(
                    self.offsets.as_ptr() as *mut u8
                )),
            ]
        }
    }

    fn offset(&self) -> usize {
        self.offset
    }

    fn children(&self) -> Vec<Arc<dyn Array>> {
        vec![self.values.clone()]
    }
}

unsafe impl<O: Offset, A: ffi::ArrowArrayRef> FromFfi<A> for ListArray<O> {
    fn try_from_ffi(array: A) -> Result<Self> {
        let data_type = array.data_type()?;
        let length = array.array().len();
        let offset = array.array().offset();
        let mut validity = unsafe { array.validity() }?;
        let mut offsets = unsafe { array.buffer::<O>(0) }?;
        let child = array.child(0)?;
        let values = ffi::try_from(child)?.into();

        if offset > 0 {
            offsets = offsets.slice(offset, length);
            validity = validity.map(|x| x.slice(offset, length))
        }
        Ok(Self::from_data(data_type, offsets, values, validity))
    }
}
