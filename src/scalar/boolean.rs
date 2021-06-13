use crate::{array::*, bitmap::Bitmap, datatypes::DataType};

use super::Scalar;

#[derive(Debug, Clone)]
pub struct BooleanScalar {
    value: bool,
    is_valid: bool,
}

impl BooleanScalar {
    #[inline]
    pub fn new(v: Option<bool>) -> Self {
        let is_valid = v.is_some();
        Self {
            value: v.unwrap_or_default(),
            is_valid,
        }
    }

    #[inline]
    pub fn value(&self) -> bool {
        self.value
    }
}

impl Scalar for BooleanScalar {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn is_valid(&self) -> bool {
        self.is_valid
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &DataType::Boolean
    }

    fn to_boxed_array(&self, length: usize) -> Box<dyn Array> {
        if self.is_valid {
            let values = Bitmap::from_trusted_len_iter(std::iter::repeat(self.value).take(length));
            Box::new(BooleanArray::from_data(values, None))
        } else {
            Box::new(BooleanArray::new_null(length))
        }
    }
}
