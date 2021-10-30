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

    fn offset(&self) -> Option<usize> {
        self.keys.offset()
    }

    fn to_ffi_aligned(&self) -> Self {
        Self {
            data_type: self.data_type.clone(),
            keys: self.keys.to_ffi_aligned(),
            values: self.values,
        }
    }
}

impl<K: DictionaryKey, A: ffi::ArrowArrayRef> FromFfi<A> for DictionaryArray<K> {
    unsafe fn try_from_ffi(array: A) -> Result<Self> {
        // keys: similar to PrimitiveArray, but the datatype is the inner one
        let length = array.array().len();
        let offset = array.array().offset();
        let mut validity = unsafe { array.validity() }?;
        let mut values = unsafe { array.buffer::<K>(0) }?;

        if offset > 0 {
            values = values.slice(offset, length);
            validity = validity.map(|x| x.slice(offset, length))
        }
        let keys = PrimitiveArray::<K>::from_data(K::DATA_TYPE, values, validity);
        // values
        let values = array.dictionary()?.unwrap();
        let values = ffi::try_from(values)?.into();

        Ok(DictionaryArray::<K>::from_data(keys, values))
    }
}
