use crate::{
    buffer::{Bitmap, Buffer},
    datatypes::DataType,
};

use super::{ffi::ToFFI, Array};

#[derive(Debug, Clone)]
pub struct FixedSizeBinaryArray {
    size: i32, // this is redundant with `data_type`, but useful to not have to deconstruct the data_type.
    data_type: DataType,
    values: Buffer<u8>,
    validity: Option<Bitmap>,
    offset: usize,
}

impl FixedSizeBinaryArray {
    pub fn new_empty(size: i32) -> Self {
        Self::from_data(size, Buffer::new(), None)
    }

    pub fn from_data(size: i32, values: Buffer<u8>, validity: Option<Bitmap>) -> Self {
        assert_eq!(values.len() % (size as usize), 0);

        Self {
            size,
            data_type: DataType::FixedSizeBinary(size),
            values,
            validity,
            offset: 0,
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.clone().map(|x| x.slice(offset, length));
        let offset = offset * self.size as usize;
        let length = offset * self.size as usize;
        Self {
            data_type: self.data_type.clone(),
            size: self.size,
            values: self.values.clone().slice(offset, length),
            validity,
            offset: 0,
        }
    }
}

impl Array for FixedSizeBinaryArray {
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

unsafe impl ToFFI for FixedSizeBinaryArray {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3] {
        unsafe {
            [
                self.validity.as_ref().map(|x| x.as_ptr()),
                Some(std::ptr::NonNull::new_unchecked(
                    self.values.as_ptr() as *mut u8
                )),
                None,
            ]
        }
    }

    fn offset(&self) -> usize {
        self.offset
    }
}
