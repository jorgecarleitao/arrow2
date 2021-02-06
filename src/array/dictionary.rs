use crate::{
    buffers::{types::NativeType, Bitmap},
    datatypes::DataType,
};

use super::{primitive::PrimitiveArray, Array};

pub unsafe trait DicionaryKey: NativeType {}

unsafe impl DicionaryKey for i8 {}
unsafe impl DicionaryKey for i16 {}
unsafe impl DicionaryKey for i32 {}
unsafe impl DicionaryKey for i64 {}
unsafe impl DicionaryKey for u8 {}
unsafe impl DicionaryKey for u16 {}
unsafe impl DicionaryKey for u32 {}
unsafe impl DicionaryKey for u64 {}

pub struct DictionaryArray<K: DicionaryKey> {
    data_type: DataType,
    keys: PrimitiveArray<K>,
    values: Box<dyn Array>,
    validity: Option<Bitmap>,
}

impl<K: DicionaryKey> DictionaryArray<K> {
    pub fn from_data(
        keys: PrimitiveArray<K>,
        values: Box<dyn Array>,
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
        }
    }
}
