//! Definition of basic div operations with primitive arrays
use std::ops::Div;

use num::{CheckedDiv, Zero};

use crate::{
    array::{Array, PrimitiveArray},
    compute::arity::{binary, binary_checked, unary, unary_checked},
    error::{ArrowError, Result},
    types::NativeType,
};

/// Divides two primitive arrays with the same type.
/// Panics if the divisor is zero of one pair of values overflows.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::div::div;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(10), Some(6)]).to(DataType::Int32);
/// let b = Primitive::from(&vec![Some(5), Some(6)]).to(DataType::Int32);
/// let result = div(&a, &b).unwrap();
/// let expected = Primitive::from(&vec![Some(2), Some(1)]).to(DataType::Int32);
/// assert_eq!(result, expected)
/// ```
#[inline]
pub fn div<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: NativeType + Div<Output = T>,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }

    binary(lhs, rhs, lhs.data_type().clone(), |a, b| a / b)
}

/// Checked division of two primitive arrays. If the result from the division
/// overflows, the result for the operation will change the validity array
/// making this operation None
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::div::checked_div;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(-100i8), Some(10i8)]).to(DataType::Int8);
/// let b = Primitive::from(&vec![Some(100i8), Some(0i8)]).to(DataType::Int8);
/// let result = checked_div(&a, &b).unwrap();
/// let expected = Primitive::from(&vec![Some(-1i8), None]).to(DataType::Int8);
/// assert_eq!(result, expected);
/// ```
pub fn checked_div<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: NativeType + CheckedDiv<Output = T> + Zero,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }

    let op = move |a: T, b: T| a.checked_div(&b);

    binary_checked(lhs, rhs, lhs.data_type().clone(), op)
}

/// Divide a primitive array of type T by a scalar T.
/// Panics if the divisor is zero.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::div::div_scalar;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
/// let result = div_scalar(&a, &2i32);
/// let expected = Primitive::from(&vec![None, Some(3), None, Some(3)]).to(DataType::Int32);
/// assert_eq!(result, expected)
/// ```
#[inline]
pub fn div_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> PrimitiveArray<T>
where
    T: NativeType + Div<Output = T>,
{
    let rhs = *rhs;
    unary(lhs, |a| a / rhs, lhs.data_type().clone())
}

/// Checked division of a primitive array of type T by a scalar T. If the
/// divisor is zero then the validity array is changed to None.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::div::checked_div_scalar;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(-100i8)]).to(DataType::Int8);
/// let result = checked_div_scalar(&a, &100i8);
/// let expected = Primitive::from(&vec![Some(-1i8)]).to(DataType::Int8);
/// assert_eq!(result, expected);
/// ```
#[inline]
pub fn checked_div_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> PrimitiveArray<T>
where
    T: NativeType + CheckedDiv<Output = T> + Zero,
{
    let rhs = *rhs;
    let op = move |a: T| a.checked_div(&rhs);

    unary_checked(lhs, op, lhs.data_type().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Primitive;
    use crate::datatypes::DataType;

    #[test]
    fn test_div_mismatched_length() {
        let a = Primitive::from_slice(vec![5, 6]).to(DataType::Int32);
        let b = Primitive::from_slice(vec![5]).to(DataType::Int32);
        div(&a, &b)
            .err()
            .expect("should have failed due to different lengths");
    }

    #[test]
    fn test_div() {
        let a = Primitive::from(&vec![Some(5), Some(6)]).to(DataType::Int32);
        let b = Primitive::from(&vec![Some(5), Some(6)]).to(DataType::Int32);
        let result = div(&a, &b).unwrap();
        let expected = Primitive::from(&vec![Some(1), Some(1)]).to(DataType::Int32);
        assert_eq!(result, expected)
    }

    #[test]
    #[should_panic]
    fn test_div_panic() {
        let a = Primitive::from(&vec![Some(10i8)]).to(DataType::Int8);
        let b = Primitive::from(&vec![Some(0i8)]).to(DataType::Int8);
        let _ = div(&a, &b);
    }

    #[test]
    fn test_div_checked() {
        let a = Primitive::from(&vec![Some(5), None, Some(3), Some(6)]).to(DataType::Int32);
        let b = Primitive::from(&vec![Some(5), Some(3), None, Some(6)]).to(DataType::Int32);
        let result = checked_div(&a, &b).unwrap();
        let expected = Primitive::from(&vec![Some(1), None, None, Some(1)]).to(DataType::Int32);
        assert_eq!(result, expected);

        let a = Primitive::from(&vec![Some(5), None, Some(3), Some(6)]).to(DataType::Int32);
        let b = Primitive::from(&vec![Some(5), Some(0), Some(0), Some(6)]).to(DataType::Int32);
        let result = checked_div(&a, &b).unwrap();
        let expected = Primitive::from(&vec![Some(1), None, None, Some(1)]).to(DataType::Int32);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_div_scalar() {
        let a = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
        let result = div_scalar(&a, &1i32);
        let expected = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
        assert_eq!(result, expected)
    }

    #[test]
    fn test_div_scalar_checked() {
        let a = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
        let result = checked_div_scalar(&a, &1i32);
        let expected = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
        assert_eq!(result, expected);

        let a = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
        let result = checked_div_scalar(&a, &0);
        let expected = Primitive::from(&vec![None, None, None, None]).to(DataType::Int32);
        assert_eq!(result, expected);
    }
}
