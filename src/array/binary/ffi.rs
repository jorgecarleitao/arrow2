use crate::{
    array::{FromFfi, Offset, ToFfi},
    datatypes::DataType,
    ffi,
};

use crate::error::Result;

use super::BinaryArray;

unsafe impl<O: Offset> ToFfi for BinaryArray<O> {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        let offset = self
            .validity
            .as_ref()
            .map(|x| x.offset())
            .unwrap_or_default();

        assert!(self.offsets.offset() >= offset);
        vec![
            self.validity.as_ref().map(|x| x.as_ptr()),
            std::ptr::NonNull::new(unsafe {
                self.offsets.as_ptr().offset(-(offset as isize)) as *mut u8
            }),
            std::ptr::NonNull::new(self.values.as_ptr() as *mut u8),
        ]
    }
}

impl<O: Offset, A: ffi::ArrowArrayRef> FromFfi<A> for BinaryArray<O> {
    unsafe fn try_from_ffi(array: A) -> Result<Self> {
        let data_type = array.field().data_type().clone();
        let expected = if O::is_large() {
            DataType::LargeBinary
        } else {
            DataType::Binary
        };
        assert_eq!(data_type, expected);

        let validity = unsafe { array.validity() }?;
        let offsets = unsafe { array.buffer::<O>(0) }?;
        let values = unsafe { array.buffer::<u8>(1) }?;

        Ok(Self::from_data_unchecked(
            Self::default_data_type(),
            offsets,
            values,
            validity,
        ))
    }
}
