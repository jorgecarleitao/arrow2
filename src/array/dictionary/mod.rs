use std::sync::Arc;

use crate::{
    buffer::{types::NativeType, Bitmap},
    datatypes::DataType,
};

use super::{ffi::ToFFI, new_empty_array, primitive::PrimitiveArray, Array};

pub unsafe trait DictionaryKey: NativeType {
    const DATA_TYPE: DataType;
}

unsafe impl DictionaryKey for i8 {
    const DATA_TYPE: DataType = DataType::Int8;
}
unsafe impl DictionaryKey for i16 {
    const DATA_TYPE: DataType = DataType::Int16;
}
unsafe impl DictionaryKey for i32 {
    const DATA_TYPE: DataType = DataType::Int32;
}
unsafe impl DictionaryKey for i64 {
    const DATA_TYPE: DataType = DataType::Int64;
}
unsafe impl DictionaryKey for u8 {
    const DATA_TYPE: DataType = DataType::UInt8;
}
unsafe impl DictionaryKey for u16 {
    const DATA_TYPE: DataType = DataType::UInt16;
}
unsafe impl DictionaryKey for u32 {
    const DATA_TYPE: DataType = DataType::UInt32;
}
unsafe impl DictionaryKey for u64 {
    const DATA_TYPE: DataType = DataType::UInt64;
}

#[derive(Debug, Clone)]
pub struct DictionaryArray<K: DictionaryKey> {
    data_type: DataType,
    keys: PrimitiveArray<K>,
    values: Arc<dyn Array>,
    validity: Option<Bitmap>,
    offset: usize,
}

impl<K: DictionaryKey> DictionaryArray<K> {
    pub fn new_empty(datatype: DataType) -> Self {
        let values = new_empty_array(datatype).into();
        Self::from_data(PrimitiveArray::<K>::new_empty(K::DATA_TYPE), values, None)
    }

    pub fn from_data(
        keys: PrimitiveArray<K>,
        values: Arc<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Self {
        let data_type = DataType::Dictionary(
            Box::new(keys.data_type().clone()),
            Box::new(values.data_type().clone()),
        );

        Self {
            data_type,
            keys,
            values,
            validity,
            offset: 0,
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.clone().map(|x| x.slice(offset, length));
        Self {
            data_type: self.data_type.clone(),
            keys: self.keys.clone().slice(offset, length),
            values: self.values.clone(),
            validity,
            offset: self.offset + offset,
        }
    }
}

impl<K: DictionaryKey> Array for DictionaryArray<K> {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn nulls(&self) -> &Option<Bitmap> {
        &self.validity
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

unsafe impl<K: DictionaryKey> ToFFI for DictionaryArray<K> {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3] {
        [self.validity.as_ref().map(|x| x.as_ptr()), None, None]
    }

    #[inline]
    fn offset(&self) -> usize {
        self.offset
    }
}
