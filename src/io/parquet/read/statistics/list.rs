use crate::array::*;
use crate::datatypes::DataType;
use crate::error::Result;

use super::make_mutable;

#[derive(Debug)]
pub struct DynMutableListArray {
    data_type: DataType,
    pub inner: Box<dyn MutableArray>,
}

impl DynMutableListArray {
    pub fn try_with_capacity(data_type: DataType, capacity: usize) -> Result<Self> {
        let inner = match data_type.to_logical_type() {
            DataType::List(inner) | DataType::LargeList(inner) => inner.data_type(),
            _ => unreachable!(),
        };
        let inner = make_mutable(inner, capacity)?;

        Ok(Self { data_type, inner })
    }
}

impl MutableArray for DynMutableListArray {
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

        match self.data_type.to_logical_type() {
            DataType::List(_) => {
                let offsets = vec![0, inner.len() as i32].into();
                Box::new(ListArray::<i32>::new(
                    self.data_type.clone(),
                    offsets,
                    inner,
                    None,
                ))
            }
            DataType::LargeList(_) => {
                let offsets = vec![0, inner.len() as i64].into();
                Box::new(ListArray::<i64>::new(
                    self.data_type.clone(),
                    offsets,
                    inner,
                    None,
                ))
            }
            _ => unreachable!(),
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
