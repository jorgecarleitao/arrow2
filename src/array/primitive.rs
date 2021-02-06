use bits::null_count;

use crate::{
    bits,
    buffers::{types::NativeType, Buffer},
    datatypes::DataType,
};

use super::Array;

#[derive(Debug)]
pub struct PrimitiveArray<T: NativeType> {
    data_type: DataType,
    values: Buffer<T>,
    validity: Option<Buffer<u8>>,
    null_count: usize,
}

impl<T: NativeType> PrimitiveArray<T> {
    pub fn from_data(data_type: DataType, values: Buffer<T>, validity: Option<Buffer<u8>>) -> Self {
        let null_count = null_count(validity.as_ref().map(|x| x.as_slice()), 0, values.len());
        Self {
            data_type,
            values,
            validity,
            null_count,
        }
    }
}

impl<T: NativeType> Array for PrimitiveArray<T> {
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

    #[inline]
    fn is_null(&self, _: usize) -> bool {
        todo!()
    }
}
