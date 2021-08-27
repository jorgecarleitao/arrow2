use crate::{
    array::{ffi as array_ffi, FromFfi, ToFfi},
    datatypes::DataType,
    error::Result,
    ffi,
};

use super::ExtensionArray;

unsafe impl ToFfi for ExtensionArray {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        array_ffi::buffers(self.inner.as_ref())
    }

    #[inline]
    fn offset(&self) -> usize {
        array_ffi::offset(self.inner.as_ref())
    }
}

unsafe impl<A: ffi::ArrowArrayRef> FromFfi<A> for ExtensionArray {
    fn try_from_ffi(array: A) -> Result<Self> {
        let inner = ffi::try_from(array)?.into();
        let data_type: DataType = todo!(); // todo: DataType from fields' metadata
        Ok(Self::from_data(data_type, inner))
    }
}
