use std::sync::Arc;

use crate::{buffer::Bitmap, datatypes::DataType};

use super::{ffi::ToFFI, new_empty_array, Array};

#[derive(Debug, Clone)]
pub struct FixedSizeListArray {
    size: i32, // this is redundant with `data_type`, but useful to not have to deconstruct the data_type.
    data_type: DataType,
    values: Arc<dyn Array>,
    validity: Option<Bitmap>,
    offset: usize,
}

impl FixedSizeListArray {
    pub fn new_empty(data_type: DataType) -> Self {
        let values = new_empty_array(Self::get_child_and_size(&data_type).0.clone()).into();
        Self::from_data(data_type, values, None)
    }

    pub fn from_data(
        data_type: DataType,
        values: Arc<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Self {
        let (_, size) = Self::get_child_and_size(&data_type);

        assert_eq!(values.len() % (*size as usize), 0);

        Self {
            size: *size,
            data_type,
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

impl FixedSizeListArray {
    pub(crate) fn get_child_and_size(data_type: &DataType) -> (&DataType, &i32) {
        if let DataType::FixedSizeList(field, size) = data_type {
            (field.data_type(), size)
        } else {
            panic!("Wrong DataType")
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
