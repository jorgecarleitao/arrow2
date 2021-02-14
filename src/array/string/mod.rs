use crate::{
    buffer::{Bitmap, Buffer},
    datatypes::DataType,
};

use super::{
    ffi::ToFFI,
    specification::{check_offsets, check_offsets_and_utf8},
    Array, Offset,
};

#[derive(Debug, Clone)]
pub struct Utf8Array<O: Offset> {
    data_type: DataType,
    offsets: Buffer<O>,
    values: Buffer<u8>,
    validity: Option<Bitmap>,
    offset: usize,
}

impl<O: Offset> Utf8Array<O> {
    pub fn new_empty() -> Self {
        unsafe { Self::from_data_unchecked(Buffer::from(&[O::zero()]), Buffer::new(), None) }
    }

    pub fn from_data(offsets: Buffer<O>, values: Buffer<u8>, validity: Option<Bitmap>) -> Self {
        check_offsets_and_utf8(&offsets, &values);

        Self {
            data_type: if O::is_large() {
                DataType::LargeUtf8
            } else {
                DataType::Utf8
            },
            offsets,
            values,
            validity,
            offset: 0,
        }
    }

    /// # Safety
    /// `values` buffer must contain valid utf8 between every `offset`
    pub unsafe fn from_data_unchecked(
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Self {
        check_offsets(&offsets, values.len());

        Self {
            data_type: if O::is_large() {
                DataType::LargeUtf8
            } else {
                DataType::Utf8
            },
            offsets,
            values,
            validity,
            offset: 0,
        }
    }

    /// Returns the element at index `i` as &str
    /// # Safety
    /// Assumes that the `i < self.len`.
    pub unsafe fn value_unchecked(&self, i: usize) -> &str {
        let offset = *self.offsets.as_ptr().add(i);
        let offset_1 = *self.offsets.as_ptr().add(i + 1);
        let length = (offset_1 - offset).to_usize().unwrap();
        let offset = offset.to_usize().unwrap();

        let slice = std::slice::from_raw_parts(self.values.as_ptr().add(offset), length);
        // todo: validate utf8 so that we can use the unsafe version
        std::str::from_utf8(slice).unwrap()
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

    /// Returns the element at index `i` as &str
    pub fn value(&self, i: usize) -> &str {
        let offsets = self.offsets.as_slice();
        let offset = offsets[i];
        let offset_1 = offsets[i + 1];
        let length = (offset_1 - offset).to_usize().unwrap();
        let offset = offset.to_usize().unwrap();

        let slice = &self.values.as_slice()[offset..offset + length];
        // todo: validate utf8 so that we can use the unsafe version
        std::str::from_utf8(slice).unwrap()
    }

    #[inline]
    pub fn offsets(&self) -> &[O] {
        self.offsets.as_slice()
    }

    #[inline]
    pub fn values(&self) -> &[u8] {
        self.values.as_slice()
    }

    #[inline]
    pub fn is_valid(&self, i: usize) -> bool {
        self.validity.as_ref().map(|x| x.get_bit(i)).unwrap_or(true)
    }
}

impl<O: Offset> Array for Utf8Array<O> {
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

unsafe impl<O: Offset> ToFFI for Utf8Array<O> {
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

    fn offset(&self) -> usize {
        self.offset
    }
}

mod from;
pub use from::*;
mod iterator;
pub use iterator::*;
