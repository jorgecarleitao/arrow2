use crate::array::*;
use crate::datatypes::{DataType, PhysicalType};
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
        match self.data_type.to_physical_type() {
            PhysicalType::Dictionary(key) => match_integer_type!(key, |$T| {
                let keys = PrimitiveArray::<$T>::from_iter((0..inner.len() as $T).map(Some));
                Box::new(DictionaryArray::<$T>::from_data(keys, inner))
            }),
            _ => todo!(),
        }
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
