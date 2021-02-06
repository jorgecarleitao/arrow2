use crate::{
    buffers::{types::NativeType, Bitmap, Buffer},
    datatypes::DataType,
};

use super::Array;

#[derive(Debug)]
pub struct PrimitiveArray<T: NativeType> {
    data_type: DataType,
    values: Buffer<T>,
    validity: Option<Bitmap>,
}

impl<T: NativeType> PrimitiveArray<T> {
    pub fn from_data(data_type: DataType, values: Buffer<T>, validity: Option<Bitmap>) -> Self {
        Self {
            data_type,
            values,
            validity,
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.as_ref().map(|x| x.slice(offset, length));
        Self {
            data_type: self.data_type.clone(),
            values: self.values.slice(offset, length),
            validity,
        }
    }

    #[inline]
    pub fn values(&self) -> &[T] {
        self.values.as_slice()
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

    fn nulls(&self) -> &Option<Bitmap> {
        &self.validity
    }
}
