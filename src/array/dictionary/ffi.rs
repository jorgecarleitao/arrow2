use crate::{
    array::{FromFfi, PrimitiveArray, ToFfi},
    error::Result,
    ffi,
};

use super::{DictionaryArray, DictionaryKey};

unsafe impl<K: DictionaryKey> ToFfi for DictionaryArray<K> {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        self.keys.buffers()
    }

    #[inline]
    fn offset(&self) -> usize {
        self.offset
    }
}

impl<K: DictionaryKey, A: ffi::ArrowArrayRef> FromFfi<A> for DictionaryArray<K> {
    unsafe fn try_from_ffi(array: A) -> Result<Self> {
        // keys: similar to PrimitiveArray, but the datatype is the inner one
        let validity = unsafe { array.validity() }?;
        let values = unsafe { array.buffer::<K>(0) }?;

        let keys = PrimitiveArray::<K>::from_data(K::DATA_TYPE, values, validity);
        // values
        let values = array.dictionary()?.unwrap();
        let values = ffi::try_from(values)?.into();

        Ok(DictionaryArray::<K>::from_data(keys, values))
    }
}
