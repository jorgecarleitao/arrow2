use bits::null_count;

use crate::{bits, buffers::Buffer, datatypes::DataType};

use super::Array;

#[derive(Debug)]
pub struct FixedSizedBinaryArray {
    size: i32, // this is redundant with `data_type`, but useful to not have to deconstruct the data_type.
    data_type: DataType,
    values: Buffer<u8>,
    validity: Option<Buffer<u8>>,
    null_count: usize,
}

impl FixedSizedBinaryArray {
    pub fn from_data(size: i32, values: Buffer<u8>, validity: Option<Buffer<u8>>) -> Self {
        assert_eq!(values.len() % (size as usize), 0);
        let len = values.len() / (size as usize);
        let null_count = null_count(validity.as_ref().map(|x| x.as_slice()), 0, len);

        Self {
            size,
            data_type: DataType::FixedSizeBinary(size),
            values,
            validity,
            null_count,
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

    #[inline]
    fn is_null(&self, _: usize) -> bool {
        todo!()
    }
}
