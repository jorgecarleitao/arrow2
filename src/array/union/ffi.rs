use std::sync::Arc;

use crate::{array::FromFfi, error::Result, ffi};

use super::super::{ffi::ToFfi, Array};
use super::UnionArray;

unsafe impl ToFfi for UnionArray {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        if let Some(offsets) = &self.offsets {
            vec![
                None,
                std::ptr::NonNull::new(self.types.as_ptr() as *mut u8),
                std::ptr::NonNull::new(offsets.as_ptr() as *mut u8),
            ]
        } else {
            vec![None, std::ptr::NonNull::new(self.types.as_ptr() as *mut u8)]
        }
    }

    fn offset(&self) -> usize {
        self.offset
    }

    fn children(&self) -> Vec<Arc<dyn Array>> {
        self.fields.clone()
    }
}

impl<A: ffi::ArrowArrayRef> FromFfi<A> for UnionArray {
    unsafe fn try_from_ffi(array: A) -> Result<Self> {
        let field = array.field();
        let data_type = field.data_type().clone();
        let fields = Self::get_fields(field.data_type());

        let types = unsafe { array.buffer::<i8>(0) }?;
        let offsets = if Self::is_sparse(&data_type) {
            None
        } else {
            Some(unsafe { array.buffer::<i32>(1) }?)
        };

        let fields = (0..fields.len())
            .map(|index| {
                let child = array.child(index)?;
                Ok(ffi::try_from(child)?.into())
            })
            .collect::<Result<Vec<Arc<dyn Array>>>>()?;

        Ok(Self::from_data(data_type, types, fields, offsets))
    }
}
