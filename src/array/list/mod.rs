use std::sync::Arc;

use crate::{
    buffer::{Bitmap, Buffer},
    datatypes::{DataType, Field},
};

use super::{
    ffi::ToFFI,
    new_empty_array,
    specification::{check_offsets, Offset},
    Array,
};

#[derive(Debug, Clone)]
pub struct ListArray<O: Offset> {
    data_type: DataType,
    offsets: Buffer<O>,
    values: Arc<dyn Array>,
    validity: Option<Bitmap>,
    offset: usize,
}

impl<O: Offset> ListArray<O> {
    pub fn new_empty(data_type: DataType) -> Self {
        let values = new_empty_array(Self::get_child(&data_type).clone()).into();
        Self::from_data(data_type, Buffer::from(&[O::zero()]), values, None)
    }

    pub fn from_data(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Arc<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Self {
        check_offsets(&offsets, values.len());

        // validate data_type
        let _ = Self::get_child(&data_type);

        Self {
            data_type,
            offsets,
            values,
            validity,
            offset: 0,
        }
    }

    /// Returns the element at index `i` as &str
    pub fn value(&self, i: usize) -> Box<dyn Array> {
        let offsets = self.offsets.as_slice();
        let offset = offsets[i];
        let offset_1 = offsets[i + 1];
        let length = (offset_1 - offset).to_usize().unwrap();

        self.values.slice(offset.to_usize().unwrap(), length)
    }

    /// Returns the element at index `i` as &str
    /// # Safety
    /// Assumes that the `i < self.len`.
    pub unsafe fn value_unchecked(&self, i: usize) -> Box<dyn Array> {
        let offset = *self.offsets.as_ptr().add(i);
        let offset_1 = *self.offsets.as_ptr().add(i + 1);
        let length = (offset_1 - offset).to_usize().unwrap();

        self.values.slice(offset.to_usize().unwrap(), length)
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

    #[inline]
    pub fn offsets_buffer(&self) -> &Buffer<O> {
        &self.offsets
    }

    #[inline]
    pub fn offsets(&self) -> &[O] {
        self.offsets.as_slice()
    }

    #[inline]
    pub fn values(&self) -> &Arc<dyn Array> {
        &self.values
    }
}

impl<O: Offset> ListArray<O> {
    pub fn default_datatype(data_type: DataType) -> DataType {
        let field = Box::new(Field::new("item", data_type, true));
        if O::is_large() {
            DataType::LargeList(field)
        } else {
            DataType::List(field)
        }
    }

    pub(crate) fn get_child(data_type: &DataType) -> &DataType {
        if O::is_large() {
            if let DataType::LargeList(child) = data_type {
                child.data_type()
            } else {
                panic!("Wrong DataType")
            }
        } else {
            if let DataType::List(child) = data_type {
                child.data_type()
            } else {
                panic!("Wrong DataType")
            }
        }
    }
}

impl<O: Offset> Array for ListArray<O> {
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
    fn nulls(&self) -> &Option<Bitmap> {
        &self.validity
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

unsafe impl<O: Offset> ToFFI for ListArray<O> {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3] {
        unsafe {
            [
                self.validity.as_ref().map(|x| x.as_ptr()),
                Some(std::ptr::NonNull::new_unchecked(
                    self.offsets.as_ptr() as *mut u8
                )),
                None,
            ]
        }
    }

    fn offset(&self) -> usize {
        self.offset
    }
}

mod from;

pub use from::ListPrimitive;

#[cfg(test)]
mod tests {
    use crate::array::primitive::PrimitiveArray;

    use super::*;

    #[test]
    fn test_create() {
        let values = Buffer::from([1, 2, 3, 4, 5]);
        let values = PrimitiveArray::<i32>::from_data(DataType::Int32, values, None);

        let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
        ListArray::<i32>::from_data(
            data_type,
            Buffer::from([0, 2, 2, 3, 5]),
            Arc::new(values),
            None,
        );
    }
}
