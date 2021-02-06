use bits::null_count;

use crate::{bits, buffers::Buffer, datatypes::DataType};

use super::{list::Offset, specification::check_offsets, Array};

#[derive(Debug)]
pub struct BinaryArray<O: Offset> {
    data_type: DataType,
    offsets: Buffer<O>,
    values: Buffer<u8>,
    validity: Option<Buffer<u8>>,
    null_count: usize,
}

impl<O: Offset> BinaryArray<O> {
    pub fn from_data(offsets: Buffer<O>, values: Buffer<u8>, validity: Option<Buffer<u8>>) -> Self {
        check_offsets(&offsets, values.len());

        let null_count = null_count(validity.as_ref().map(|x| x.as_slice()), 0, values.len());

        Self {
            data_type: if O::is_large() {
                DataType::LargeBinary
            } else {
                DataType::Binary
            },
            offsets,
            values,
            validity,
            null_count,
        }
    }
}

impl<O: Offset> Array for BinaryArray<O> {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.offsets.len() - 1
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
