use crate::{
    buffer::{Bitmap, Buffer},
    datatypes::DataType,
};

use super::Array;

#[derive(Debug)]
pub struct FixedSizedBinaryArray {
    size: i32, // this is redundant with `data_type`, but useful to not have to deconstruct the data_type.
    data_type: DataType,
    values: Buffer<u8>,
    validity: Option<Bitmap>,
}

impl FixedSizedBinaryArray {
    pub fn from_data(size: i32, values: Buffer<u8>, validity: Option<Bitmap>) -> Self {
        assert_eq!(values.len() % (size as usize), 0);

        Self {
            size,
            data_type: DataType::FixedSizeBinary(size),
            values,
            validity,
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.as_ref().map(|x| x.slice(offset, length));
        let offset = offset * self.size as usize;
        let length = offset * self.size as usize;
        Self {
            data_type: self.data_type.clone(),
            size: self.size,
            values: self.values.slice(offset, length),
            validity,
        }
    }
}

impl Array for FixedSizedBinaryArray {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.values.len() / self.size as usize
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
