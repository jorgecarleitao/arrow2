use crate::array::*;
use crate::datatypes::DataType;
use crate::error::Result;

use super::make_mutable;

#[derive(Debug)]
pub struct DynMutableDictionary {
    data_type: DataType,
    pub inner: Box<dyn MutableArray>,
}

impl DynMutableDictionary {
    pub fn try_with_capacity(data_type: DataType, capacity: usize) -> Result<Self> {
        let inner = if let DataType::Dictionary(_, inner, _) = &data_type {
            inner.as_ref()
        } else {
            unreachable!()
        };
        let inner = make_mutable(inner, capacity)?;

        Ok(Self { data_type, inner })
    }
}

impl MutableArray for DynMutableDictionary {
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn validity(&self) -> Option<&crate::bitmap::MutableBitmap> {
        self.inner.validity()
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        let inner = self.inner.as_arc();
        let keys = PrimitiveArray::<i32>::from_iter((0..inner.len() as i32).map(Some));
        Box::new(DictionaryArray::<i32>::from_data(keys, inner))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn push_null(&mut self) {
        todo!()
    }

    fn shrink_to_fit(&mut self) {
        todo!()
    }
}
