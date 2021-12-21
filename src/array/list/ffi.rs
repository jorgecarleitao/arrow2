use std::sync::Arc;

use crate::{array::FromFfi, bitmap::align, error::Result, ffi};

use super::super::{ffi::ToFfi, Array, Offset};
use super::ListArray;

unsafe impl<O: Offset> ToFfi for ListArray<O> {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        vec![
            self.validity.as_ref().map(|x| x.as_ptr()),
            Some(self.offsets.as_ptr().cast::<u8>()),
        ]
    }

    fn children(&self) -> Vec<Arc<dyn Array>> {
        vec![self.values.clone()]
    }

    fn offset(&self) -> Option<usize> {
        let offset = self.offsets.offset();
        if let Some(bitmap) = self.validity.as_ref() {
            if bitmap.offset() == offset {
                Some(offset)
            } else {
                None
            }
        } else {
            Some(offset)
        }
    }

    fn to_ffi_aligned(&self) -> Self {
        let offset = self.offsets.offset();

        let validity = self.validity.as_ref().map(|bitmap| {
            if bitmap.offset() == offset {
                bitmap.clone()
            } else {
                align(bitmap, offset)
            }
        });

        Self {
            data_type: self.data_type.clone(),
            validity,
            offsets: self.offsets.clone(),
            values: self.values.clone(),
        }
    }
}

impl<O: Offset, A: ffi::ArrowArrayRef> FromFfi<A> for ListArray<O> {
    unsafe fn try_from_ffi(array: A) -> Result<Self> {
        let data_type = array.field().data_type().clone();
        let validity = unsafe { array.validity() }?;
        let offsets = unsafe { array.buffer::<O>(1) }?;
        let child = unsafe { array.child(0)? };
        let values = ffi::try_from(child)?.into();

        Ok(Self::from_data(data_type, offsets, values, validity))
    }
}
