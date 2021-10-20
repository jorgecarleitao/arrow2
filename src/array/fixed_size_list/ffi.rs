use std::sync::Arc;

use crate::array::ffi::ToFfi;
use crate::array::Array;

use super::FixedSizeListArray;

unsafe impl ToFfi for FixedSizeListArray {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        vec![self.validity.as_ref().map(|x| x.as_ptr())]
    }

    fn children(&self) -> Vec<Arc<dyn Array>> {
        vec![self.values().clone()]
    }
}
