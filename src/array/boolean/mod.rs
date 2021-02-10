use crate::{buffer::Bitmap, datatypes::DataType, ffi::ArrowArray};

use super::{ffi::ToFFI, Array, FromFFI};

use crate::error::Result;

#[derive(Debug, Clone)]
pub struct BooleanArray {
    data_type: DataType,
    values: Bitmap,
    validity: Option<Bitmap>,
    offset: usize,
}

impl BooleanArray {
    pub fn new_empty() -> Self {
        Self::from_data(Bitmap::new(), None)
    }

    pub fn from_data(values: Bitmap, validity: Option<Bitmap>) -> Self {
        Self {
            data_type: DataType::Boolean,
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

    /// Returns the element at index `i` as &str
    pub fn value(&self, i: usize) -> bool {
        self.values.get_bit(i)
    }

    pub fn values(&self) -> &Bitmap {
        &self.values
    }
}

impl Array for BooleanArray {
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

unsafe impl ToFFI for BooleanArray {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3] {
        [
            self.validity.as_ref().map(|x| x.as_ptr()),
            Some(self.values.as_ptr()),
            None,
        ]
    }

    fn offset(&self) -> usize {
        self.offset
    }
}

unsafe impl FromFFI for BooleanArray {
    fn try_from_ffi(data_type: DataType, array: ArrowArray) -> Result<Self> {
        let length = array.len();
        let offset = array.offset();
        let mut validity = array.null_bit_buffer();
        let mut values = unsafe { array.bitmap(0)? };

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

impl<P: AsRef<[Option<bool>]>> From<P> for BooleanArray {
    fn from(slice: P) -> Self {
        unsafe { Self::from_trusted_len_iter(slice.as_ref().iter().map(|x| x.as_ref())) }
    }
}

mod iterator;
pub use iterator::*;

mod from;
