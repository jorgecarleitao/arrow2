use crate::{
    array::{FromFfi, Offset, ToFfi},
    datatypes::DataType,
    ffi,
};

use crate::error::Result;

use super::BinaryArray;

unsafe impl<O: Offset> ToFfi for BinaryArray<O> {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        vec![
            self.validity.as_ref().map(|x| x.as_ptr()),
            std::ptr::NonNull::new(self.offsets.as_ptr() as *mut u8),
            std::ptr::NonNull::new(self.values.as_ptr() as *mut u8),
        ]
    }

    #[inline]
    fn offset(&self) -> usize {
        self.offset
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

        let length = array.array().len();
        let offset = array.array().offset();
        let mut validity = unsafe { array.validity() }?;
        let mut offsets = unsafe { array.buffer::<O>(0) }?;
        let values = unsafe { array.buffer::<u8>(1) }?;

        if offset > 0 {
            offsets = offsets.slice(offset, length);
            validity = validity.map(|x| x.slice(offset, length))
        }

        Ok(Self::from_data_unchecked(
            Self::default_data_type(),
            offsets,
            values,
            validity,
        ))
    }
}
