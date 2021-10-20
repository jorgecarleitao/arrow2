use crate::array::ffi::ToFfi;

use super::FixedSizeBinaryArray;

unsafe impl ToFfi for FixedSizeBinaryArray {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        let offset = self
            .validity
            .as_ref()
            .map(|x| x.offset())
            .unwrap_or_default();
        let offset = offset * self.size();

        // note that this may point to _before_ the pointer. This is fine because the C data interface
        // requires users to only access past the offset
        let values =
            std::ptr::NonNull::new(
                unsafe { self.values.as_ptr().offset(-(offset as isize)) } as *mut u8
            );

        vec![self.validity.as_ref().map(|x| x.as_ptr()), values]
    }
}
