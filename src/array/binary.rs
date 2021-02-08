use crate::{
    buffer::{Bitmap, Buffer},
    datatypes::DataType,
    ffi::ArrowArray,
};

use super::{ffi::ToFFI, specification::check_offsets, specification::Offset, Array, FromFFI};

use crate::error::Result;

#[derive(Debug)]
pub struct BinaryArray<O: Offset> {
    data_type: DataType,
    offsets: Buffer<O>,
    values: Buffer<u8>,
    validity: Option<Bitmap>,
    offset: usize,
}

impl<O: Offset> BinaryArray<O> {
    pub fn new_empty() -> Self {
        Self::from_data(Buffer::from(&[O::zero()]), Buffer::new(), None)
    }

    pub fn from_data(offsets: Buffer<O>, values: Buffer<u8>, validity: Option<Bitmap>) -> Self {
        check_offsets(&offsets, values.len());

        Self {
            data_type: if O::is_large() {
                DataType::LargeBinary
            } else {
                DataType::Binary
            },
            offsets,
            values,
            validity,
            offset: 0,
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.clone().map(|x| x.slice(offset, length));
        Self {
            data_type: self.data_type.clone(),
            offsets: self.offsets.clone().slice(offset, length),
            values: self.values.clone(),
            validity,
            offset: self.offset + offset,
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

    fn nulls(&self) -> &Option<Bitmap> {
        &self.validity
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

unsafe impl<O: Offset> ToFFI for BinaryArray<O> {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3] {
        unsafe {
            [
                self.validity.as_ref().map(|x| x.as_ptr()),
                Some(std::ptr::NonNull::new_unchecked(
                    self.offsets.as_ptr() as *mut u8
                )),
                Some(std::ptr::NonNull::new_unchecked(
                    self.values.as_ptr() as *mut u8
                )),
            ]
        }
    }

    #[inline]
    fn offset(&self) -> usize {
        self.offset
    }
}

unsafe impl<O: Offset> FromFFI for BinaryArray<O> {
    fn try_from_ffi(data_type: DataType, array: ArrowArray) -> Result<Self> {
        let expected = if O::is_large() {
            DataType::LargeBinary
        } else {
            DataType::Binary
        };
        assert_eq!(data_type, expected);

        let length = array.len();
        let offset = array.offset();
        let mut validity = array.null_bit_buffer();
        let mut offsets = unsafe { array.buffer::<O>(0)? };
        let values = unsafe { array.buffer::<u8>(1)? };

        if offset > 0 {
            offsets = offsets.slice(offset, length);
            validity = validity.map(|x| x.slice(offset, length))
        }

        Ok(Self {
            data_type,
            offsets,
            values,
            validity,
            offset: 0,
        })
    }
}
