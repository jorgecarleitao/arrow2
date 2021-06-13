use crate::{array::*, datatypes::DataType};

use super::Scalar;

#[derive(Debug, Clone)]
pub struct NullScalar {}

impl NullScalar {
    #[inline]
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for NullScalar {
    fn default() -> Self {
        Self::new()
    }
}

impl Scalar for NullScalar {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn is_valid(&self) -> bool {
        false
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &DataType::Null
    }

    #[inline]
    fn to_boxed_array(&self, length: usize) -> Box<dyn Array> {
        Box::new(NullArray::from_data(length))
    }
}
