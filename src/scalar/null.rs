use crate::datatypes::DataType;

use super::Scalar;

#[derive(Debug, Clone, PartialEq)]
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
}
