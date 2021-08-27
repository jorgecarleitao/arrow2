use std::sync::Arc;

use crate::{
    array::{new_empty_array, new_null_array, Array},
    bitmap::Bitmap,
    datatypes::DataType,
};

mod ffi;

#[derive(Debug, Clone)]
pub struct ExtensionArray {
    data_type: DataType,
    inner: Arc<dyn Array>,
}

impl ExtensionArray {
    pub fn from_data(data_type: DataType, inner: Arc<dyn Array>) -> Self {
        if let DataType::Extension(_, inner_data_type, _) = &data_type {
            assert_eq!(inner_data_type.as_ref(), inner.data_type())
        } else {
            panic!("Extension array requires DataType::Extension")
        }
        Self { data_type, inner }
    }

    pub fn new_empty(data_type: DataType) -> Self {
        if let DataType::Extension(_, inner_data_type, _) = &data_type {
            let inner = new_empty_array(inner_data_type.as_ref().clone()).into();
            Self::from_data(data_type, inner)
        } else {
            panic!("Extension array requires DataType::Extension")
        }
    }

    pub fn new_null(data_type: DataType, length: usize) -> Self {
        if let DataType::Extension(_, inner_data_type, _) = &data_type {
            let inner = new_null_array(inner_data_type.as_ref().clone(), length).into();
            Self::from_data(data_type, inner)
        } else {
            panic!("Extension array requires DataType::Extension")
        }
    }

    pub fn inner(&self) -> &dyn Array {
        self.inner.as_ref()
    }
}

impl Array for ExtensionArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn validity(&self) -> &Option<Bitmap> {
        self.inner.validity()
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Array::slice(self.inner.as_ref(), offset, length)
    }
}
