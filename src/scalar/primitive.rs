use crate::{
    datatypes::DataType,
    types::{NativeType, NaturalDataType},
    error::ArrowError,
};

use super::Scalar;

/// The implementation of [`Scalar`] for primitive, semantically equivalent to [`Option<T>`]
/// with [`DataType`].
#[derive(Debug, Clone)]
pub struct PrimitiveScalar<T: NativeType> {
    // Not Option<T> because this offers a stabler pointer offset on the struct
    value: T,
    is_valid: bool,
    data_type: DataType,
}

impl<T: NativeType> PartialEq for PrimitiveScalar<T> {
    fn eq(&self, other: &Self) -> bool {
        self.data_type == other.data_type
            && self.is_valid == other.is_valid
            && ((!self.is_valid) | (self.value == other.value))
    }
}

impl<T: NativeType> PrimitiveScalar<T> {
    /// Returns a new [`PrimitiveScalar`].
    #[inline]
    pub fn new(data_type: DataType, v: Option<T>) -> Self {
        if !T::is_valid(&data_type) {
            Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<T>(),
                data_type
            )))
            .unwrap()
        }
        let is_valid = v.is_some();
        Self {
            value: v.unwrap_or_default(),
            is_valid,
            data_type,
        }
    }

    /// Returns the value irrespectively of its validity.
    #[inline]
    pub fn value(&self) -> T {
        self.value
    }

    /// Returns a new `PrimitiveScalar` with the same value but different [`DataType`]
    /// # Panic
    /// This function panics if the `data_type` is not valid for self's physical type `T`.
    pub fn to(self, data_type: DataType) -> Self {
        let v = if self.is_valid {
            Some(self.value)
        } else {
            None
        };
        Self::new(data_type, v)
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
        self.is_valid
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}
