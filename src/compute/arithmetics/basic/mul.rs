//! Definition of basic mul operations with primitive arrays
use std::ops::Mul;

use num_traits::{ops::overflowing::OverflowingMul, CheckedMul, SaturatingMul, WrappingMul, Zero};

use crate::compute::arithmetics::basic::check_same_type;
use crate::compute::arithmetics::ArrayWrappingMul;
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
    error::Result,
    types::NativeType,
};

/// Multiplies two primitive arrays with the same type.
/// Panics if the multiplication of one pair of values overflows.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::mul;
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
    check_same_type(lhs, rhs)?;

    binary(lhs, rhs, lhs.data_type().clone(), |a, b| a * b)
}

/// Wrapping multiplication of two primitive arrays.
/// It do nothing if the result overflows.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::wrapping_mul;
/// use arrow2::array::PrimitiveArray;
///
/// let a = PrimitiveArray::from([Some(100i8), Some(100i8), Some(100i8)]);
/// let b = PrimitiveArray::from([Some(0i8), Some(100i8), Some(0i8)]);
/// let result = wrapping_mul(&a, &b).unwrap();
/// let expected = PrimitiveArray::from([Some(0), Some(0), Some(0)]);
/// assert_eq!(result, expected);
/// ```
pub fn wrapping_mul<T>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: NativeType + WrappingMul<Output = T>,
{
    check_same_type(lhs, rhs)?;

    let op = move |a: T, b: T| a.wrapping_mul(&b);

    binary(lhs, rhs, lhs.data_type().clone(), op)
}

/// Checked multiplication of two primitive arrays. If the result from the
/// multiplications overflows, the validity for that index is changed
/// returned.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::checked_mul;
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
    check_same_type(lhs, rhs)?;

    let op = move |a: T, b: T| a.checked_mul(&b);

    binary_checked(lhs, rhs, lhs.data_type().clone(), op)
}

/// Saturating multiplication of two primitive arrays. If the result from the
/// multiplication overflows, the result for the
/// operation will be the saturated value.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::saturating_mul;
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
    check_same_type(lhs, rhs)?;

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
/// use arrow2::compute::arithmetics::basic::overflowing_mul;
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
    check_same_type(lhs, rhs)?;

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

// Implementation of ArrayWrappingMul trait for PrimitiveArrays
impl<T> ArrayWrappingMul<PrimitiveArray<T>> for PrimitiveArray<T>
where
    T: NativeType + WrappingMul<Output = T> + NotI128,
{
    type Output = Self;

    fn wrapping_mul(&self, rhs: &PrimitiveArray<T>) -> Result<Self::Output> {
        wrapping_mul(self, rhs)
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
/// use arrow2::compute::arithmetics::basic::mul_scalar;
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

/// Wrapping multiplication of a scalar T to a primitive array of type T.
/// It do nothing if the result overflows.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::wrapping_mul_scalar;
/// use arrow2::array::Int8Array;
///
/// let a = Int8Array::from(&[None, Some(0x10)]);
/// let result = wrapping_mul_scalar(&a, &0x10);
/// let expected = Int8Array::from(&[None, Some(0)]);
/// assert_eq!(result, expected);
/// ```
pub fn wrapping_mul_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> PrimitiveArray<T>
where
    T: NativeType + WrappingMul<Output = T>,
{
    unary(lhs, |a| a.wrapping_mul(rhs), lhs.data_type().clone())
}

/// Checked multiplication of a scalar T to a primitive array of type T. If the
/// result from the multiplication overflows, then the validity for that index is
/// changed to None
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::checked_mul_scalar;
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
/// use arrow2::compute::arithmetics::basic::saturating_mul_scalar;
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
/// use arrow2::compute::arithmetics::basic::overflowing_mul_scalar;
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
