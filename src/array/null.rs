use crate::{buffer::Bitmap, datatypes::DataType};

use super::{ffi::ToFFI, Array};

#[derive(Debug, Clone)]
pub struct NullArray {
    data_type: DataType,
    length: usize,
    offset: usize,
}

impl NullArray {
    pub fn new_empty() -> Self {
        Self::from_data(0)
    }

    pub fn from_data(length: usize) -> Self {
        Self {
            data_type: DataType::Null,
            length,
            offset: 0,
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            length,
            offset: self.offset + offset,
        }
    }
}

impl Array for NullArray {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.length
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &DataType::Null
    }

    fn nulls(&self) -> &Option<Bitmap> {
        &None
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

unsafe impl ToFFI for NullArray {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3] {
        [None, None, None]
    }

    #[inline]
    fn offset(&self) -> usize {
        self.offset
    }
}
