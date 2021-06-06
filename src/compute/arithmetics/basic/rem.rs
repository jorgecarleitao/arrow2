use std::ops::Rem;

use num::{traits::CheckedRem, Zero};

use crate::{
    array::{Array, PrimitiveArray},
    compute::{
        arithmetics::{ArrayCheckedRem, ArrayRem, NotI128},
        arity::{binary, binary_checked, unary, unary_checked},
    },
    error::{ArrowError, Result},
    types::NativeType,
};

/// Remainder of two primitive arrays with the same type.
/// Panics if the divisor is zero of one pair of values overflows.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::rem::rem;
/// use arrow2::array::Int32Array;
///
/// let a = Int32Array::from(&[Some(10), Some(7)]);
/// let b = Int32Array::from(&[Some(5), Some(6)]);
/// let result = rem(&a, &b).unwrap();
/// let expected = Int32Array::from(&[Some(0), Some(1)]);
/// assert_eq!(result, expected)
/// ```
pub fn rem<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: NativeType + Rem<Output = T>,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }

    binary(lhs, rhs, lhs.data_type().clone(), |a, b| a % b)
}

/// Checked remainder of two primitive arrays. If the result from the remainder
/// overflows, the result for the operation will change the validity array
/// making this operation None
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::rem::checked_rem;
/// use arrow2::array::Int8Array;
///
/// let a = Int8Array::from(&[Some(-100i8), Some(10i8)]);
/// let b = Int8Array::from(&[Some(100i8), Some(0i8)]);
/// let result = checked_rem(&a, &b).unwrap();
/// let expected = Int8Array::from(&[Some(-0i8), None]);
/// assert_eq!(result, expected);
/// ```
pub fn checked_rem<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: NativeType + CheckedRem<Output = T> + Zero,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }

    let op = move |a: T, b: T| a.checked_rem(&b);

    binary_checked(lhs, rhs, lhs.data_type().clone(), op)
}

impl<T> ArrayRem<PrimitiveArray<T>> for PrimitiveArray<T>
where
    T: NativeType + Rem<Output = T> + NotI128,
{
    type Output = Self;

    fn rem(&self, rhs: &PrimitiveArray<T>) -> Result<Self::Output> {
        rem(self, rhs)
    }
}

impl<T> ArrayCheckedRem<PrimitiveArray<T>> for PrimitiveArray<T>
where
    T: NativeType + CheckedRem<Output = T> + Zero + NotI128,
{
    type Output = Self;

    fn checked_rem(&self, rhs: &PrimitiveArray<T>) -> Result<Self::Output> {
        checked_rem(self, rhs)
    }
}

/// Remainder a primitive array of type T by a scalar T.
/// Panics if the divisor is zero.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::rem::rem_scalar;
/// use arrow2::array::Int32Array;
///
/// let a = Int32Array::from(&[None, Some(6), None, Some(7)]);
/// let result = rem_scalar(&a, &2i32);
/// let expected = Int32Array::from(&[None, Some(0), None, Some(1)]);
/// assert_eq!(result, expected)
/// ```
pub fn rem_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> PrimitiveArray<T>
where
    T: NativeType + Rem<Output = T>,
{
    let rhs = *rhs;
    unary(lhs, |a| a % rhs, lhs.data_type().clone())
}

/// Checked remainder of a primitive array of type T by a scalar T. If the
/// divisor is zero then the validity array is changed to None.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::rem::checked_rem_scalar;
/// use arrow2::array::Int8Array;
///
/// let a = Int8Array::from(&[Some(-100i8)]);
/// let result = checked_rem_scalar(&a, &100i8);
/// let expected = Int8Array::from(&[Some(0i8)]);
/// assert_eq!(result, expected);
/// ```
pub fn checked_rem_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> PrimitiveArray<T>
where
    T: NativeType + CheckedRem<Output = T> + Zero,
{
    let rhs = *rhs;
    let op = move |a: T| a.checked_rem(&rhs);

    unary_checked(lhs, op, lhs.data_type().clone())
}

impl<T> ArrayRem<T> for PrimitiveArray<T>
where
    T: NativeType + Rem<Output = T> + NotI128,
{
    type Output = Self;

    fn rem(&self, rhs: &T) -> Result<Self::Output> {
        Ok(rem_scalar(self, rhs))
    }
}

impl<T> ArrayCheckedRem<T> for PrimitiveArray<T>
where
    T: NativeType + CheckedRem<Output = T> + Zero + NotI128,
{
    type Output = Self;

    fn checked_rem(&self, rhs: &T) -> Result<Self::Output> {
        Ok(checked_rem_scalar(self, rhs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::*;

    #[test]
    fn test_rem_mismatched_length() {
        let a = Int32Array::from_slice(&[5, 6]);
        let b = Int32Array::from_slice(&[5]);
        rem(&a, &b)
            .err()
            .expect("should have failed due to different lengths");
    }

    #[test]
    fn test_rem() {
        let a = Int32Array::from(&[Some(5), Some(6)]);
        let b = Int32Array::from(&[Some(4), Some(4)]);
        let result = rem(&a, &b).unwrap();
        let expected = Int32Array::from(&[Some(1), Some(2)]);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.rem(&b).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    #[should_panic]
    fn test_rem_panic() {
        let a = Int8Array::from(&[Some(10i8)]);
        let b = Int8Array::from(&[Some(0i8)]);
        let _ = rem(&a, &b);
    }

    #[test]
    fn test_rem_checked() {
        let a = Int32Array::from(&[Some(5), None, Some(3), Some(6)]);
        let b = Int32Array::from(&[Some(5), Some(3), None, Some(5)]);
        let result = checked_rem(&a, &b).unwrap();
        let expected = Int32Array::from(&[Some(0), None, None, Some(1)]);
        assert_eq!(result, expected);

        let a = Int32Array::from(&[Some(5), None, Some(3), Some(6)]);
        let b = Int32Array::from(&[Some(5), Some(0), Some(0), Some(5)]);
        let result = checked_rem(&a, &b).unwrap();
        let expected = Int32Array::from(&[Some(0), None, None, Some(1)]);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.checked_rem(&b).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_rem_scalar() {
        let a = Int32Array::from(&[None, Some(6), None, Some(5)]);
        let result = rem_scalar(&a, &2i32);
        let expected = Int32Array::from(&[None, Some(0), None, Some(1)]);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.rem(&2i32).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_rem_scalar_checked() {
        let a = Int32Array::from(&[None, Some(6), None, Some(7)]);
        let result = checked_rem_scalar(&a, &2i32);
        let expected = Int32Array::from(&[None, Some(0), None, Some(1)]);
        assert_eq!(result, expected);

        let a = Int32Array::from(&[None, Some(6), None, Some(6)]);
        let result = checked_rem_scalar(&a, &0);
        let expected = Int32Array::from(&[None, None, None, None]);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.checked_rem(&0).unwrap();
        assert_eq!(result, expected);
    }
}
