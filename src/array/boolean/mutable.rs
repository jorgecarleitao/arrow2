use std::sync::Arc;

use crate::{
    array::{Array, MutableArray},
    bitmap::MutableBitmap,
    datatypes::DataType,
};

use super::BooleanArray;

/// The mutable version of [`BooleanArray`].
#[derive(Debug)]
pub struct MutableBooleanArray {
    values: MutableBitmap,
    validity: Option<MutableBitmap>,
}

impl From<MutableBooleanArray> for BooleanArray {
    fn from(other: MutableBooleanArray) -> Self {
        BooleanArray::from_data(other.values.into(), other.validity.map(|x| x.into()))
    }
}

impl Default for MutableBooleanArray {
    fn default() -> Self {
        Self::new()
    }
}

impl MutableBooleanArray {
    pub fn new() -> Self {
        Self {
            values: MutableBitmap::new(),
            validity: None,
        }
    }

    pub fn push(&mut self, value: Option<bool>) {
        match value {
            Some(value) => {
                self.values.push(value);
                match &mut self.validity {
                    Some(validity) => validity.push(true),
                    None => {
                        self.set_validity();
                    }
                }
            }
            None => {
                self.values.push(false);
                match &mut self.validity {
                    Some(validity) => validity.push(false),
                    None => {}
                }
            }
        }
    }

    fn set_validity(&mut self) {
        self.validity = Some(MutableBitmap::from_trusted_len_iter(
            std::iter::repeat(false)
                .take(self.len() - 1)
                .chain(std::iter::once(true)),
        ))
    }
}

impl MutableArray for MutableBooleanArray {
    fn len(&self) -> usize {
        self.values.len()
    }

    fn validity(&self) -> &Option<MutableBitmap> {
        &self.validity
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(BooleanArray::from_data(
            std::mem::take(&mut self.values).into(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        ))
    }

    fn data_type(&self) -> &DataType {
        &DataType::Boolean
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    #[inline]
    fn push_null(&mut self) {
        self.push(None)
    }
}
