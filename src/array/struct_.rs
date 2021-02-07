use std::sync::Arc;

use crate::{
    buffer::Bitmap,
    datatypes::{DataType, Field},
};

use super::Array;

#[derive(Debug)]
pub struct StructArray {
    data_type: DataType,
    values: Vec<Arc<dyn Array>>,
    validity: Option<Bitmap>,
}

impl StructArray {
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
        let validity = self.validity.as_ref().map(|x| x.slice(offset, length));
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
