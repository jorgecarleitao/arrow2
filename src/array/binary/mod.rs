use crate::{bitmap::Bitmap, buffer::Buffer, datatypes::DataType};

use super::{
    display_fmt, display_helper, specification::check_offsets, specification::Offset, Array,
    GenericBinaryArray,
};

mod ffi;
mod iterator;
pub use iterator::*;
mod from;
mod mutable;
pub use mutable::*;

#[derive(Debug, Clone)]
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

    #[inline]
    pub fn new_null(length: usize) -> Self {
        Self::from_data(
            Buffer::new_zeroed(length + 1),
            Buffer::new(),
            Some(Bitmap::new_zeroed(length)),
        )
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
        let offsets = self.offsets.clone().slice(offset, length + 1);
        Self {
            data_type: self.data_type.clone(),
            offsets,
            values: self.values.clone(),
            validity,
            offset: self.offset + offset,
        }
    }

    /// Returns the element at index `i` as &str
    pub fn value(&self, i: usize) -> &[u8] {
        let offsets = self.offsets.as_slice();
        let offset = offsets[i];
        let offset_1 = offsets[i + 1];
        let length = (offset_1 - offset).to_usize();
        let offset = offset.to_usize();

        &self.values.as_slice()[offset..offset + length]
    }

    /// Returns the element at index `i` as &str
    /// # Safety
    /// Assumes that the `i < self.len`.
    pub unsafe fn value_unchecked(&self, i: usize) -> &[u8] {
        let offset = *self.offsets.as_ptr().add(i);
        let offset_1 = *self.offsets.as_ptr().add(i + 1);
        let length = (offset_1 - offset).to_usize();
        let offset = offset.to_usize();

        std::slice::from_raw_parts(self.values.as_ptr().add(offset), length)
    }

    #[inline]
    pub fn offsets(&self) -> &Buffer<O> {
        &self.offsets
    }

    #[inline]
    pub fn values(&self) -> &Buffer<u8> {
        &self.values
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

    fn validity(&self) -> &Option<Bitmap> {
        &self.validity
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

impl<O: Offset> std::fmt::Display for BinaryArray<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let a = |x: &[u8]| display_helper(x.iter().map(|x| Some(format!("{:b}", x)))).join(" ");
        let iter = self.iter().map(|x| x.map(a));
        let head = if O::is_large() {
            "LargeBinaryArray"
        } else {
            "BinaryArray"
        };
        display_fmt(iter, head, f, false)
    }
}

unsafe impl<O: Offset> GenericBinaryArray<O> for BinaryArray<O> {
    #[inline]
    fn values(&self) -> &[u8] {
        self.values()
    }

    #[inline]
    fn offsets(&self) -> &[O] {
        self.offsets()
    }
}
