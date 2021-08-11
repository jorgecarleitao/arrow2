use crate::datatypes::DataType;

use super::Scalar;

#[derive(Debug, Clone)]
pub struct BooleanScalar {
    value: bool,
    is_valid: bool,
}

impl PartialEq for BooleanScalar {
    fn eq(&self, other: &Self) -> bool {
        self.is_valid == other.is_valid && ((!self.is_valid) | (self.value == other.value))
    }
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
}

impl From<Option<bool>> for BooleanScalar {
    #[inline]
    fn from(v: Option<bool>) -> Self {
        Self::new(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::eq_op)]
    #[test]
    fn equal() {
        let a = BooleanScalar::from(Some(true));
        let b = BooleanScalar::from(None);
        assert_eq!(a, a);
        assert_eq!(b, b);
        assert!(a != b);
        let b = BooleanScalar::from(Some(false));
        assert!(a != b);
        assert_eq!(b, b);
    }

    #[test]
    fn basics() {
        let a = BooleanScalar::new(Some(true));

        assert!(a.value());
        assert_eq!(a.data_type(), &DataType::Boolean);
        assert!(a.is_valid());
    }
}
