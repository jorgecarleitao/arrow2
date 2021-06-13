use std::any::Any;
use std::sync::Arc;

use crate::{
    array::*,
    buffer::Buffer,
    datatypes::{DataType, Field},
};

use super::Scalar;

/// The scalar equivalent of [`ListArray`]. Like [`ListArray`], this struct holds a dynamically-typed
/// [`Array`]. The only difference is that this has only one element.
#[derive(Debug, Clone)]
pub struct ListScalar<O: Offset> {
    values: Arc<dyn Array>,
    length: O,
    is_valid: bool,
    phantom: std::marker::PhantomData<O>,
    data_type: DataType,
}

pub enum ListScalarNew {
    Array(Arc<dyn Array>),
    DataType(DataType),
}

impl<O: Offset> ListScalar<O> {
    #[inline]
    pub fn new(v: ListScalarNew) -> Self {
        let (data_type, values, is_valid) = match v {
            ListScalarNew::Array(a) => (a.data_type().clone(), a, true),
            ListScalarNew::DataType(d) => (d.clone(), new_empty_array(d).into(), false),
        };
        let field = Field::new("item", data_type, true);
        let data_type = if O::is_large() {
            DataType::LargeList(Box::new(field))
        } else {
            DataType::List(Box::new(field))
        };
        let length = O::from_usize(values.len()).unwrap();
        Self {
            values,
            length,
            is_valid,
            phantom: std::marker::PhantomData,
            data_type,
        }
    }
}

impl<O: Offset> Scalar for ListScalar<O> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn to_boxed_array(&self, length: usize) -> Box<dyn Array> {
        if self.is_valid {
            let offsets = (0..=length).map(|i| O::from_usize(i).unwrap() * self.length);
            let offsets = unsafe { Buffer::from_trusted_len_iter_unchecked(offsets) };
            let values = std::iter::repeat(self.values.as_ref())
                .take(self.length.to_usize().unwrap())
                .collect::<Vec<_>>();
            let values = crate::compute::concat::concatenate(&values).unwrap();
            Box::new(ListArray::<O>::from_data(
                self.data_type.clone(),
                offsets,
                values.into(),
                None,
            ))
        } else {
            Box::new(ListArray::<O>::new_null(self.data_type.clone(), length))
        }
    }
}
