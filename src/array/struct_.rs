use std::sync::Arc;

use crate::{
    buffer::Bitmap,
    datatypes::{DataType, Field},
};

use super::{ffi::ToFFI, new_empty_array, Array};

#[derive(Debug)]
pub struct StructArray {
    data_type: DataType,
    values: Vec<Arc<dyn Array>>,
    validity: Option<Bitmap>,
}

impl StructArray {
    pub fn new_empty(fields: &[Field]) -> Self {
        let values = fields
            .iter()
            .map(|field| new_empty_array(field.data_type().clone()).into())
            .collect();
        Self::from_data(fields.to_vec(), values, None)
    }

    pub fn from_data(
        fields: Vec<Field>,
        values: Vec<Arc<dyn Array>>,
        validity: Option<Bitmap>,
    ) -> Self {
        Self {
            data_type: DataType::Struct(fields),
            values,
            validity,
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.clone().map(|x| x.slice(offset, length));
        Self {
            data_type: self.data_type.clone(),
            values: self
                .values
                .iter()
                .map(|x| x.slice(offset, length).into())
                .collect(),
            validity,
        }
    }

    #[inline]
    pub fn values(&self) -> &[Arc<dyn Array>] {
        &self.values
    }
}

impl Array for StructArray {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.values[0].len()
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

unsafe impl ToFFI for StructArray {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3] {
        [self.validity.as_ref().map(|x| x.as_ptr()), None, None]
    }

    fn offset(&self) -> usize {
        // we do not support offsets in structs. Instead, if an FFI we slice the incoming arrays
        0
    }
}
