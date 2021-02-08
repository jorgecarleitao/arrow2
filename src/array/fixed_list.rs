use std::sync::Arc;

use crate::{
    buffer::Bitmap,
    datatypes::{DataType, Field},
};

use super::{ffi::ToFFI, new_empty_array, Array};

#[derive(Debug)]
pub struct FixedSizeListArray {
    size: i32, // this is redundant with `data_type`, but useful to not have to deconstruct the data_type.
    data_type: DataType,
    values: Arc<dyn Array>,
    validity: Option<Bitmap>,
    offset: usize,
}

impl FixedSizeListArray {
    pub fn new_empty(size: i32, field: Field) -> Self {
        Self::from_data(
            size,
            new_empty_array(field.data_type().clone()).into(),
            None,
            Some((field.name(), field.is_nullable())),
        )
    }

    pub fn from_data(
        size: i32,
        values: Arc<dyn Array>,
        validity: Option<Bitmap>,
        field_options: Option<(&str, bool)>,
    ) -> Self {
        assert_eq!(values.len() % (size as usize), 0);

        let (field_name, field_nullable) = field_options.unwrap_or(("item", true));

        let field = Box::new(Field::new(
            field_name,
            values.data_type().clone(),
            field_nullable,
        ));

        Self {
            size,
            data_type: DataType::FixedSizeList(field, size),
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
            values: self.values.clone().slice(offset, length).into(),
            validity,
            offset: 0,
        }
    }
}

impl Array for FixedSizeListArray {
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

unsafe impl ToFFI for FixedSizeListArray {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3] {
        [self.validity.as_ref().map(|x| x.as_ptr()), None, None]
    }

    fn offset(&self) -> usize {
        self.offset
    }
}
