use crate::{
    buffer::{types::NativeType, Bitmap, Buffer},
    datatypes::DataType,
    ffi::ArrowArray,
};

use crate::error::Result;

use super::{
    ffi::{FromFFI, ToFFI},
    Array,
};

#[derive(Debug, Clone)]
pub struct PrimitiveArray<T: NativeType> {
    data_type: DataType,
    values: Buffer<T>,
    validity: Option<Bitmap>,
    offset: usize,
}

impl<T: NativeType> PrimitiveArray<T> {
    pub fn new_empty(data_type: DataType) -> Self {
        Self::from_data(data_type, Buffer::new(), None)
    }

    pub fn from_data(data_type: DataType, values: Buffer<T>, validity: Option<Bitmap>) -> Self {
        assert!(T::is_valid(&data_type));
        Self {
            data_type,
            values,
            validity,
            offset: 0,
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.clone().map(|x| x.slice(offset, length));
        Self {
            data_type: self.data_type.clone(),
            values: self.values.clone().slice(offset, length),
            validity,
            offset: self.offset + offset,
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

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

unsafe impl<T: NativeType> ToFFI for PrimitiveArray<T> {
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

    #[inline]
    fn offset(&self) -> usize {
        self.offset
    }
}

unsafe impl<T: NativeType> FromFFI for PrimitiveArray<T> {
    fn try_from_ffi(data_type: DataType, array: ArrowArray) -> Result<Self> {
        let length = array.len();
        let offset = array.offset();
        let mut validity = array.null_bit_buffer();
        let mut values = unsafe { array.buffer::<T>(0)? };

        if offset > 0 {
            values = values.slice(offset, length);
            validity = validity.map(|x| x.slice(offset, length))
        }
        Ok(Self {
            data_type,
            values,
            validity,
            offset: 0,
        })
    }
}

impl<T: NativeType, P: AsRef<[Option<T>]>> From<(DataType, P)> for PrimitiveArray<T> {
    fn from((datatype, slice): (DataType, P)) -> Self {
        unsafe { Self::from_trusted_len_iter(datatype, slice.as_ref().iter().map(|x| x.as_ref())) }
    }
}

mod from;
mod iterator;
pub use iterator::*;
