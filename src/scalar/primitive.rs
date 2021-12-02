use crate::{
    datatypes::DataType,
    error::ArrowError,
    types::{NativeType, NaturalDataType},
};

use super::Scalar;

/// The implementation of [`Scalar`] for primitive, semantically equivalent to [`Option<T>`]
/// with [`DataType`].
#[derive(Debug, Clone, PartialEq)]
pub struct PrimitiveScalar<T: NativeType> {
    value: Option<T>,
    data_type: DataType,
}

impl<T: NativeType> PrimitiveScalar<T> {
    /// Returns a new [`PrimitiveScalar`].
    #[inline]
    pub fn new(data_type: DataType, value: Option<T>) -> Self {
        if !T::is_valid(&data_type) {
            Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<T>(),
                data_type
            )))
            .unwrap()
        }
        Self { value, data_type }
    }

    /// Returns the optional value.
    #[inline]
    pub fn value(&self) -> Option<T> {
        self.value
    }

    /// Returns a new `PrimitiveScalar` with the same value but different [`DataType`]
    /// # Panic
    /// This function panics if the `data_type` is not valid for self's physical type `T`.
    pub fn to(self, data_type: DataType) -> Self {
        Self::new(data_type, self.value)
    }
}

impl<T: NativeType + NaturalDataType> From<Option<T>> for PrimitiveScalar<T> {
    #[inline]
    fn from(v: Option<T>) -> Self {
        Self::new(T::DATA_TYPE, v)
    }
}

impl<T: NativeType> Scalar for PrimitiveScalar<T> {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn is_valid(&self) -> bool {
        self.value.is_some()
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}
