use crate::datatypes::DataType;

use super::Scalar;

#[derive(Debug, Clone, PartialEq)]
/// The [`Scalar`] implementation of fixed size binary ([`Option<Vec<u8>>`]).
pub struct FixedSizeBinaryScalar {
    value: Option<Vec<u8>>,
    data_type: DataType,
}

impl FixedSizeBinaryScalar {
    /// Returns a new [`FixedSizeBinaryScalar`].
    /// # Panics
    /// iff
    /// * the `data_type` is not `FixedSizeBinary`
    /// * the size of child binary is not equal
    #[inline]
    pub fn new<P: Into<Vec<u8>>>(data_type: DataType, value: Option<P>) -> Self {
        Self {
            value: value.map(|x| {
                let x = x.into();
                assert_eq!(data_type, DataType::FixedSizeBinary(x.len()));
                x
            }),
            data_type,
        }
    }

    /// Its value
    #[inline]
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_ref().map(|x| x.as_ref())
    }
}

impl Scalar for FixedSizeBinaryScalar {
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
