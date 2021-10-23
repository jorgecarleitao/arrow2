use std::sync::Arc;

use crate::{array::FromFfi, error::Result, ffi};

use super::super::{ffi::ToFfi, specification::Offset, Array};
use super::ListArray;

unsafe impl<O: Offset> ToFfi for ListArray<O> {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        let offset = self
            .validity
            .as_ref()
            .map(|x| x.offset())
            .unwrap_or_default();

        // note that this may point to _before_ the pointer. This is fine because the C data interface
        // requires users to only access past the offset
        let offsets =
            std::ptr::NonNull::new(
                unsafe { self.offsets.as_ptr().offset(-(offset as isize)) } as *mut u8
            );

        vec![self.validity.as_ref().map(|x| x.as_ptr()), offsets]
    }

    fn children(&self) -> Vec<Arc<dyn Array>> {
        vec![self.values.clone()]
    }
}

impl<O: Offset, A: ffi::ArrowArrayRef> FromFfi<A> for ListArray<O> {
    unsafe fn try_from_ffi(array: A) -> Result<Self> {
        let data_type = array.field().data_type().clone();
        let validity = unsafe { array.validity() }?;
        let offsets = unsafe { array.buffer::<O>(0) }?;
        let child = array.child(0)?;
        let values = ffi::try_from(child)?.into();

        Ok(Self::from_data(data_type, offsets, values, validity))
    }
}
