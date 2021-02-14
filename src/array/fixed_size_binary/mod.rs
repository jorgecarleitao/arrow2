use crate::{
    buffer::{Bitmap, Buffer},
    datatypes::DataType,
};

use super::{display_fmt, display_helper, ffi::ToFFI, Array};

mod iterator;

#[derive(Debug, Clone)]
pub struct FixedSizeBinaryArray {
    size: i32, // this is redundant with `data_type`, but useful to not have to deconstruct the data_type.
    data_type: DataType,
    values: Buffer<u8>,
    validity: Option<Bitmap>,
    offset: usize,
}

impl FixedSizeBinaryArray {
    pub fn new_empty(data_type: DataType) -> Self {
        Self::from_data(data_type, Buffer::new(), None)
    }

    pub fn from_data(data_type: DataType, values: Buffer<u8>, validity: Option<Bitmap>) -> Self {
        let size = *Self::get_size(&data_type);

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

    #[inline]
    pub fn values(&self) -> &[u8] {
        self.values.as_slice()
    }

    /// Returns the element at index `i` as &str
    /// # Safety
    /// Assumes that the `i < self.len`.
    pub unsafe fn value_unchecked(&self, i: usize) -> &[u8] {
        std::slice::from_raw_parts(
            self.values.as_ptr().add(i * self.size as usize),
            self.size as usize,
        )
    }
}

impl FixedSizeBinaryArray {
    pub(crate) fn get_size(data_type: &DataType) -> &i32 {
        if let DataType::FixedSizeBinary(size) = data_type {
            size
        } else {
            panic!("Wrong DataType")
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

impl std::fmt::Display for FixedSizeBinaryArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let a = |x: &[u8]| display_helper(x.iter().map(|x| Some(format!("{:b}", x)))).join(" ");
        let iter = self.iter().map(|x| x.map(a));
        display_fmt(iter, "FixedSizeBinaryArray", f, false)
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
