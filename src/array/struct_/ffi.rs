use std::sync::Arc;

use super::super::{ffi::ToFfi, Array, FromFfi};
use super::StructArray;
use crate::{error::Result, ffi};

unsafe impl ToFfi for StructArray {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        vec![self.validity.as_ref().map(|x| x.as_ptr())]
    }

    fn children(&self) -> Vec<Arc<dyn Array>> {
        self.values.clone()
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

impl<A: ffi::ArrowArrayRef> FromFfi<A> for StructArray {
    unsafe fn try_from_ffi(array: A) -> Result<Self> {
        let data_type = array.data_type().clone();
        let fields = Self::get_fields(&data_type);

        let validity = unsafe { array.validity() }?;
        let values = (0..fields.len())
            .map(|index| {
                let child = array.child(index)?;
                Ok(ffi::try_from(child)?.into())
            })
            .collect::<Result<Vec<Arc<dyn Array>>>>()?;

        Self::try_new(data_type, values, validity)
    }
}
