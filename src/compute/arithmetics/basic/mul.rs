use std::ops::Mul;

use crate::{
    array::{Array, PrimitiveArray},
    compute::arity::{binary, unary},
    error::{ArrowError, Result},
    types::NativeType,
};

#[inline]
pub fn mul<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: NativeType + Mul<Output = T>,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }

    binary(lhs, rhs, lhs.data_type().clone(), |lhs, rhs| lhs * rhs)
}

#[inline]
pub fn mul_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> PrimitiveArray<T>
where
    T: NativeType + Mul<Output = T>,
{
    let rhs = *rhs;
    unary(lhs, |lhs| lhs * rhs, lhs.data_type())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Primitive;
    use crate::datatypes::DataType;

    #[test]
    fn test_multiply() {
        let a = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
        let b = Primitive::from(&vec![Some(5), None, None, Some(6)]).to(DataType::Int32);
        let result = mul(&a, &b).unwrap();
        let expected = Primitive::from(&vec![None, None, None, Some(36)]).to(DataType::Int32);
        assert_eq!(result, expected)
    }
}
