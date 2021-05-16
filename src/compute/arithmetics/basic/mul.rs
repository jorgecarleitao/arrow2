//! Definition of basic mul operations with primitive arrays
use std::ops::Mul;

use num::{
    traits::{ops::overflowing::OverflowingMul, SaturatingMul},
    CheckedMul, Zero,
};

use crate::{
    array::{Array, PrimitiveArray},
    bitmap::Bitmap,
    compute::{
        arithmetics::{
            ArrayCheckedMul, ArrayMul, ArrayOverflowingMul, ArraySaturatingMul, NotI128,
        },
        arity::{
            binary, binary_checked, binary_with_bitmap, unary, unary_checked, unary_with_bitmap,
        },
    },
    error::{ArrowError, Result},
    types::NativeType,
};

/// Multiplies two primitive arrays with the same type.
/// Panics if the multiplication of one pair of values overflows.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::mul::mul;
/// use arrow2::array::Int32Array;
///
/// let a = Int32Array::from(&[None, Some(6), None, Some(6)]);
/// let b = Int32Array::from(&[Some(5), None, None, Some(6)]);
/// let result = mul(&a, &b).unwrap();
/// let expected = Int32Array::from(&[None, None, None, Some(36)]);
/// assert_eq!(result, expected)
/// ```
pub fn mul<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: NativeType + Mul<Output = T>,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }

    binary(lhs, rhs, lhs.data_type().clone(), |a, b| a * b)
}

/// Checked multiplication of two primitive arrays. If the result from the
/// multiplications overflows, the validity for that index is changed
/// returned.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::mul::checked_mul;
/// use arrow2::array::Int8Array;
///
/// let a = Int8Array::from(&[Some(100i8), Some(100i8), Some(100i8)]);
/// let b = Int8Array::from(&[Some(1i8), Some(100i8), Some(1i8)]);
/// let result = checked_mul(&a, &b).unwrap();
/// let expected = Int8Array::from(&[Some(100i8), None, Some(100i8)]);
/// assert_eq!(result, expected);
/// ```
pub fn checked_mul<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: NativeType + CheckedMul<Output = T> + Zero,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }

    let op = move |a: T, b: T| a.checked_mul(&b);

    binary_checked(lhs, rhs, lhs.data_type().clone(), op)
}

/// Saturating multiplication of two primitive arrays. If the result from the
/// multiplication overflows, the result for the
/// operation will be the saturated value.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::mul::saturating_mul;
/// use arrow2::array::Int8Array;
///
/// let a = Int8Array::from(&[Some(-100i8)]);
/// let b = Int8Array::from(&[Some(100i8)]);
/// let result = saturating_mul(&a, &b).unwrap();
/// let expected = Int8Array::from(&[Some(-128)]);
/// assert_eq!(result, expected);
/// ```
pub fn saturating_mul<T>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: NativeType + SaturatingMul<Output = T>,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }

    let op = move |a: T, b: T| a.saturating_mul(&b);

    binary(lhs, rhs, lhs.data_type().clone(), op)
}

/// Overflowing multiplication of two primitive arrays. If the result from the
/// mul overflows, the result for the operation will be an array with
/// overflowed values and a validity array indicating the overflowing elements
/// from the array.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::mul::overflowing_mul;
/// use arrow2::array::Int8Array;
///
/// let a = Int8Array::from(&[Some(1i8), Some(-100i8)]);
/// let b = Int8Array::from(&[Some(1i8), Some(100i8)]);
/// let (result, overflow) = overflowing_mul(&a, &b).unwrap();
/// let expected = Int8Array::from(&[Some(1i8), Some(-16i8)]);
/// assert_eq!(result, expected);
/// ```
pub fn overflowing_mul<T>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
) -> Result<(PrimitiveArray<T>, Bitmap)>
where
    T: NativeType + OverflowingMul<Output = T>,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }

    let op = move |a: T, b: T| a.overflowing_mul(&b);

    binary_with_bitmap(lhs, rhs, lhs.data_type().clone(), op)
}

// Implementation of ArrayMul trait for PrimitiveArrays
impl<T> ArrayMul<PrimitiveArray<T>> for PrimitiveArray<T>
where
    T: NativeType + Mul<Output = T> + NotI128,
{
    type Output = Self;

    fn mul(&self, rhs: &PrimitiveArray<T>) -> Result<Self::Output> {
        mul(self, rhs)
    }
}

// Implementation of ArrayCheckedMul trait for PrimitiveArrays
impl<T> ArrayCheckedMul<PrimitiveArray<T>> for PrimitiveArray<T>
where
    T: NativeType + CheckedMul<Output = T> + Zero + NotI128,
{
    type Output = Self;

    fn checked_mul(&self, rhs: &PrimitiveArray<T>) -> Result<Self::Output> {
        checked_mul(self, rhs)
    }
}

// Implementation of ArraySaturatingMul trait for PrimitiveArrays
impl<T> ArraySaturatingMul<PrimitiveArray<T>> for PrimitiveArray<T>
where
    T: NativeType + SaturatingMul<Output = T> + NotI128,
{
    type Output = Self;

    fn saturating_mul(&self, rhs: &PrimitiveArray<T>) -> Result<Self::Output> {
        saturating_mul(self, rhs)
    }
}

// Implementation of ArraySaturatingMul trait for PrimitiveArrays
impl<T> ArrayOverflowingMul<PrimitiveArray<T>> for PrimitiveArray<T>
where
    T: NativeType + OverflowingMul<Output = T> + NotI128,
{
    type Output = Self;

    fn overflowing_mul(&self, rhs: &PrimitiveArray<T>) -> Result<(Self::Output, Bitmap)> {
        overflowing_mul(self, rhs)
    }
}
/// Multiply a scalar T to a primitive array of type T.
/// Panics if the multiplication of the values overflows.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::mul::mul_scalar;
/// use arrow2::array::Int32Array;
///
/// let a = Int32Array::from(&[None, Some(6), None, Some(6)]);
/// let result = mul_scalar(&a, &2i32);
/// let expected = Int32Array::from(&[None, Some(12), None, Some(12)]);
/// assert_eq!(result, expected)
/// ```
pub fn mul_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> PrimitiveArray<T>
where
    T: NativeType + Mul<Output = T>,
{
    let rhs = *rhs;
    unary(lhs, |a| a * rhs, lhs.data_type().clone())
}

/// Checked multiplication of a scalar T to a primitive array of type T. If the
/// result from the multiplication overflows, then the validity for that index is
/// changed to None
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::mul::checked_mul_scalar;
/// use arrow2::array::Int8Array;
///
/// let a = Int8Array::from(&[None, Some(100), None, Some(100)]);
/// let result = checked_mul_scalar(&a, &100i8);
/// let expected = Int8Array::from(&[None, None, None, None]);
/// assert_eq!(result, expected);
/// ```
pub fn checked_mul_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> PrimitiveArray<T>
where
    T: NativeType + CheckedMul<Output = T> + Zero,
{
    let rhs = *rhs;
    let op = move |a: T| a.checked_mul(&rhs);

    unary_checked(lhs, op, lhs.data_type().clone())
}

/// Saturated multiplication of a scalar T to a primitive array of type T. If the
/// result from the mul overflows for this type, then
/// the result will be saturated
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::mul::saturating_mul_scalar;
/// use arrow2::array::Int8Array;
///
/// let a = Int8Array::from(&[Some(-100i8)]);
/// let result = saturating_mul_scalar(&a, &100i8);
/// let expected = Int8Array::from(&[Some(-128i8)]);
/// assert_eq!(result, expected);
/// ```
pub fn saturating_mul_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> PrimitiveArray<T>
where
    T: NativeType + SaturatingMul<Output = T>,
{
    let rhs = *rhs;
    let op = move |a: T| a.saturating_mul(&rhs);

    unary(lhs, op, lhs.data_type().clone())
}

/// Overflowing multiplication of a scalar T to a primitive array of type T. If
/// the result from the mul overflows for this type,
/// then the result will be an array with overflowed values and a validity
/// array indicating the overflowing elements from the array
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::mul::overflowing_mul_scalar;
/// use arrow2::array::Int8Array;
///
/// let a = Int8Array::from(&[Some(1i8), Some(100i8)]);
/// let (result, overflow) = overflowing_mul_scalar(&a, &100i8);
/// let expected = Int8Array::from(&[Some(100i8), Some(16i8)]);
/// assert_eq!(result, expected);
/// ```
pub fn overflowing_mul_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> (PrimitiveArray<T>, Bitmap)
where
    T: NativeType + OverflowingMul<Output = T>,
{
    let rhs = *rhs;
    let op = move |a: T| a.overflowing_mul(&rhs);

    unary_with_bitmap(lhs, op, lhs.data_type().clone())
}

// Implementation of ArrayMul trait for PrimitiveArrays with a scalar
impl<T> ArrayMul<T> for PrimitiveArray<T>
where
    T: NativeType + Mul<Output = T> + NotI128,
{
    type Output = Self;

    fn mul(&self, rhs: &T) -> Result<Self::Output> {
        Ok(mul_scalar(self, rhs))
    }
}

// Implementation of ArrayCheckedMul trait for PrimitiveArrays with a scalar
impl<T> ArrayCheckedMul<T> for PrimitiveArray<T>
where
    T: NativeType + CheckedMul<Output = T> + Zero + NotI128,
{
    type Output = Self;

    fn checked_mul(&self, rhs: &T) -> Result<Self::Output> {
        Ok(checked_mul_scalar(self, rhs))
    }
}

// Implementation of ArraySaturatingMul trait for PrimitiveArrays with a scalar
impl<T> ArraySaturatingMul<T> for PrimitiveArray<T>
where
    T: NativeType + SaturatingMul<Output = T> + NotI128,
{
    type Output = Self;

    fn saturating_mul(&self, rhs: &T) -> Result<Self::Output> {
        Ok(saturating_mul_scalar(self, rhs))
    }
}

// Implementation of ArraySaturatingMul trait for PrimitiveArrays with a scalar
impl<T> ArrayOverflowingMul<T> for PrimitiveArray<T>
where
    T: NativeType + OverflowingMul<Output = T> + NotI128,
{
    type Output = Self;

    fn overflowing_mul(&self, rhs: &T) -> Result<(Self::Output, Bitmap)> {
        Ok(overflowing_mul_scalar(self, rhs))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::*;

    #[test]
    fn test_mul_mismatched_length() {
        let a = Int32Array::from_slice(&[5, 6]);
        let b = Int32Array::from_slice(&[5]);
        mul(&a, &b)
            .err()
            .expect("should have failed due to different lengths");
    }

    #[test]
    fn test_mul() {
        let a = Int32Array::from(&[None, Some(6), None, Some(6)]);
        let b = Int32Array::from(&[Some(5), None, None, Some(6)]);
        let result = mul(&a, &b).unwrap();
        let expected = Int32Array::from(&[None, None, None, Some(36)]);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.mul(&b).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    #[should_panic]
    fn test_mul_panic() {
        let a = Int8Array::from(&[Some(-100i8)]);
        let b = Int8Array::from(&[Some(100i8)]);
        let _ = mul(&a, &b);
    }

    #[test]
    fn test_mul_checked() {
        let a = Int32Array::from(&[None, Some(6), None, Some(6)]);
        let b = Int32Array::from(&[Some(5), None, None, Some(6)]);
        let result = checked_mul(&a, &b).unwrap();
        let expected = Int32Array::from(&[None, None, None, Some(36)]);
        assert_eq!(result, expected);

        let a = Int8Array::from(&[Some(100i8), Some(100i8), Some(100i8)]);
        let b = Int8Array::from(&[Some(1i8), Some(100i8), Some(1i8)]);
        let result = checked_mul(&a, &b).unwrap();
        let expected = Int8Array::from(&[Some(100i8), None, Some(100i8)]);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.checked_mul(&b).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_mul_saturating() {
        let a = Int32Array::from(&[None, Some(6), None, Some(6)]);
        let b = Int32Array::from(&[Some(5), None, None, Some(6)]);
        let result = saturating_mul(&a, &b).unwrap();
        let expected = Int32Array::from(&[None, None, None, Some(36)]);
        assert_eq!(result, expected);

        let a = Int8Array::from(&[Some(-100i8)]);
        let b = Int8Array::from(&[Some(100i8)]);
        let result = saturating_mul(&a, &b).unwrap();
        let expected = Int8Array::from(&[Some(-128)]);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.saturating_mul(&b).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_mul_overflowing() {
        let a = Int32Array::from(&[None, Some(6), None, Some(6)]);
        let b = Int32Array::from(&[Some(5), None, None, Some(6)]);
        let (result, overflow) = overflowing_mul(&a, &b).unwrap();
        let expected = Int32Array::from(&[None, None, None, Some(36)]);
        assert_eq!(result, expected);
        assert_eq!(overflow.as_slice()[0], 0b0000);

        let a = Int8Array::from(&[Some(1i8), Some(-100i8)]);
        let b = Int8Array::from(&[Some(1i8), Some(100i8)]);
        let (result, overflow) = overflowing_mul(&a, &b).unwrap();
        let expected = Int8Array::from(&[Some(1i8), Some(-16i8)]);
        assert_eq!(result, expected);
        assert_eq!(overflow.as_slice()[0], 0b10);

        // Trait testing
        let (result, overflow) = a.overflowing_mul(&b).unwrap();
        assert_eq!(result, expected);
        assert_eq!(overflow.as_slice()[0], 0b10);
    }

    #[test]
    fn test_mul_scalar() {
        let a = Int32Array::from(&[None, Some(6), None, Some(6)]);
        let result = mul_scalar(&a, &1i32);
        let expected = Int32Array::from(&[None, Some(6), None, Some(6)]);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.mul(&1i32).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_mul_scalar_checked() {
        let a = Int32Array::from(&[None, Some(6), None, Some(6)]);
        let result = checked_mul_scalar(&a, &1i32);
        let expected = Int32Array::from(&[None, Some(6), None, Some(6)]);
        assert_eq!(result, expected);

        let a = Int8Array::from(&[None, Some(100), None, Some(100)]);
        let result = checked_mul_scalar(&a, &100i8);
        let expected = Int8Array::from(&[None, None, None, None]);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.checked_mul(&100i8).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_mul_scalar_saturating() {
        let a = Int32Array::from(&[None, Some(6), None, Some(6)]);
        let result = saturating_mul_scalar(&a, &1i32);
        let expected = Int32Array::from(&[None, Some(6), None, Some(6)]);
        assert_eq!(result, expected);

        let a = Int8Array::from(&[Some(-100i8)]);
        let result = saturating_mul_scalar(&a, &100i8);
        let expected = Int8Array::from(&[Some(-128)]);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.saturating_mul(&100i8).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_mul_scalar_overflowing() {
        let a = Int32Array::from(&[None, Some(6), None, Some(6)]);
        let (result, overflow) = overflowing_mul_scalar(&a, &1i32);
        let expected = Int32Array::from(&[None, Some(6), None, Some(6)]);
        assert_eq!(result, expected);
        assert_eq!(overflow.as_slice()[0], 0b0000);

        let a = Int8Array::from(&[Some(1i8), Some(-100i8)]);
        let (result, overflow) = overflowing_mul_scalar(&a, &100i8);
        let expected = Int8Array::from(&[Some(100i8), Some(-16i8)]);
        assert_eq!(result, expected);
        assert_eq!(overflow.as_slice()[0], 0b10);

        // Trait testing
        let (result, overflow) = a.overflowing_mul(&100i8).unwrap();
        assert_eq!(result, expected);
        assert_eq!(overflow.as_slice()[0], 0b10);
    }
}
