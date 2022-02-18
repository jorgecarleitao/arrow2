use std::sync::Arc;

use super::FixedSizeListArray;
use crate::{
    array::{
        ffi::{FromFfi, ToFfi},
        Array,
    },
    error::Result,
    ffi,
};

unsafe impl ToFfi for FixedSizeListArray {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        vec![self.validity.as_ref().map(|x| x.as_ptr())]
    }

    fn children(&self) -> Vec<Arc<dyn Array>> {
        vec![self.values().clone()]
    }

    fn offset(&self) -> Option<usize> {
        Some(
            self.validity
                .as_ref()
                .map(|bitmap| bitmap.offset())
                .unwrap_or_default(),
        )
    }

    fn to_ffi_aligned(&self) -> Self {
        self.clone()
    }
}

impl<A: ffi::ArrowArrayRef> FromFfi<A> for FixedSizeListArray {
    unsafe fn try_from_ffi(array: A) -> Result<Self> {
        let data_type = array.data_type().clone();
        let validity = unsafe { array.validity() }?;
        let child = unsafe { array.child(0)? };
        let values = ffi::try_from(child)?.into();

        Ok(Self::from_data(data_type, values, validity))
    }
}
