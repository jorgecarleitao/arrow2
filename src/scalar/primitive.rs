use crate::{array::*, buffer::Buffer, datatypes::DataType, types::NativeType};

use super::Scalar;

#[derive(Debug, Clone)]
pub struct PrimitiveScalar<T: NativeType> {
    // Not Option<T> because this offers a stabler pointer offset on the struct
    value: T,
    is_valid: bool,
    data_type: DataType,
}

impl<T: NativeType> PrimitiveScalar<T> {
    #[inline]
    pub fn new(data_type: DataType, v: Option<T>) -> Self {
        let is_valid = v.is_some();
        Self {
            value: v.unwrap_or_default(),
            is_valid,
            data_type,
        }
    }

    #[inline]
    pub fn value(&self) -> T {
        self.value
    }
}

impl<T: NativeType> Scalar for PrimitiveScalar<T> {
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
        &self.data_type
    }

    #[inline]
    fn to_boxed_array(&self, length: usize) -> Box<dyn Array> {
        if self.is_valid {
            let values = Buffer::from_trusted_len_iter(std::iter::repeat(self.value).take(length));
            Box::new(PrimitiveArray::from_data(
                self.data_type.clone(),
                values,
                None,
            ))
        } else {
            Box::new(PrimitiveArray::<T>::new_null(
                self.data_type.clone(),
                length,
            ))
        }
    }
}
