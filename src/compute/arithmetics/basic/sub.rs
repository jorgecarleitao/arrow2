//! Definition of basic sub operations with primitive arrays
use std::ops::Sub;

use num::{
    traits::{ops::overflowing::OverflowingSub, SaturatingSub},
    CheckedSub, Zero,
};

use crate::{
    array::{Array, PrimitiveArray},
    bitmap::Bitmap,
    compute::{
        arithmetics::{
            ArrayCheckedSub, ArrayOverflowingSub, ArraySaturatingSub, ArraySub, NotI128,
        },
        arity::{
            binary, binary_checked, binary_with_bitmap, unary, unary_checked, unary_with_bitmap,
        },
    },
    error::{ArrowError, Result},
    types::NativeType,
};

/// Subtracts two primitive arrays with the same type.
/// Panics if the subtraction of one pair of values overflows.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::sub::sub;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
/// let b = Primitive::from(&vec![Some(5), None, None, Some(6)]).to(DataType::Int32);
/// let result = sub(&a, &b).unwrap();
/// let expected = Primitive::from(&vec![None, None, None, Some(0)]).to(DataType::Int32);
/// assert_eq!(result, expected)
/// ```
#[inline]
pub fn sub<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: NativeType + Sub<Output = T>,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }

    binary(lhs, rhs, lhs.data_type().clone(), |a, b| a - b)
}

/// Checked subtraction of two primitive arrays. If the result from the
/// subtraction overflow, the validity for that index is changed
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::sub::checked_sub;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(100i8), Some(-100i8), Some(100i8)]).to(DataType::Int8);
/// let b = Primitive::from(&vec![Some(1i8), Some(100i8), Some(0i8)]).to(DataType::Int8);
/// let result = checked_sub(&a, &b).unwrap();
/// let expected = Primitive::from(&vec![Some(99i8), None, Some(100i8)]).to(DataType::Int8);
/// assert_eq!(result, expected);
/// ```
pub fn checked_sub<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: NativeType + CheckedSub<Output = T> + Zero,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }

    let op = move |a: T, b: T| a.checked_sub(&b);

    binary_checked(lhs, rhs, lhs.data_type().clone(), op)
}

/// Saturating subtraction of two primitive arrays. If the result from the sub
/// is smaller than the possible number for this type, the result for the
/// operation will be the saturated value.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::sub::saturating_sub;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(-100i8)]).to(DataType::Int8);
/// let b = Primitive::from(&vec![Some(100i8)]).to(DataType::Int8);
/// let result = saturating_sub(&a, &b).unwrap();
/// let expected = Primitive::from(&vec![Some(-128)]).to(DataType::Int8);
/// assert_eq!(result, expected);
/// ```
pub fn saturating_sub<T>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: NativeType + SaturatingSub<Output = T>,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }

    let op = move |a: T, b: T| a.saturating_sub(&b);

    binary(lhs, rhs, lhs.data_type().clone(), op)
}

/// Overflowing subtraction of two primitive arrays. If the result from the sub
/// is smaller than the possible number for this type, the result for the
/// operation will be an array with overflowed values and a validity array
/// indicating the overflowing elements from the array.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::sub::overflowing_sub;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(1i8), Some(-100i8)]).to(DataType::Int8);
/// let b = Primitive::from(&vec![Some(1i8), Some(100i8)]).to(DataType::Int8);
/// let (result, overflow) = overflowing_sub(&a, &b).unwrap();
/// let expected = Primitive::from(&vec![Some(0i8), Some(56i8)]).to(DataType::Int8);
/// assert_eq!(result, expected);
/// ```
pub fn overflowing_sub<T>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
) -> Result<(PrimitiveArray<T>, Bitmap)>
where
    T: NativeType + OverflowingSub<Output = T>,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }

    let op = move |a: T, b: T| a.overflowing_sub(&b);

    binary_with_bitmap(lhs, rhs, lhs.data_type().clone(), op)
}

// Implementation of ArraySub trait for PrimitiveArrays
impl<T> ArraySub<PrimitiveArray<T>> for PrimitiveArray<T>
where
    T: NativeType + Sub<Output = T> + NotI128,
{
    type Output = Self;

    fn sub(&self, rhs: &PrimitiveArray<T>) -> Result<Self::Output> {
        sub(self, rhs)
    }
}

// Implementation of ArrayCheckedSub trait for PrimitiveArrays
impl<T> ArrayCheckedSub<PrimitiveArray<T>> for PrimitiveArray<T>
where
    T: NativeType + CheckedSub<Output = T> + Zero + NotI128,
{
    type Output = Self;

    fn checked_sub(&self, rhs: &PrimitiveArray<T>) -> Result<Self::Output> {
        checked_sub(self, rhs)
    }
}

// Implementation of ArraySaturatingSub trait for PrimitiveArrays
impl<T> ArraySaturatingSub<PrimitiveArray<T>> for PrimitiveArray<T>
where
    T: NativeType + SaturatingSub<Output = T> + NotI128,
{
    type Output = Self;

    fn saturating_sub(&self, rhs: &PrimitiveArray<T>) -> Result<Self::Output> {
        saturating_sub(self, rhs)
    }
}

// Implementation of ArraySaturatingSub trait for PrimitiveArrays
impl<T> ArrayOverflowingSub<PrimitiveArray<T>> for PrimitiveArray<T>
where
    T: NativeType + OverflowingSub<Output = T>,
{
    type Output = Self;

    fn overflowing_sub(&self, rhs: &PrimitiveArray<T>) -> Result<(Self::Output, Bitmap)> {
        overflowing_sub(self, rhs)
    }
}

/// Subtract a scalar T to a primitive array of type T.
/// Panics if the subtraction of the values overflows.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::sub::sub_scalar;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
/// let result = sub_scalar(&a, &1i32);
/// let expected = Primitive::from(&vec![None, Some(5), None, Some(5)]).to(DataType::Int32);
/// assert_eq!(result, expected)
/// ```
#[inline]
pub fn sub_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> PrimitiveArray<T>
where
    T: NativeType + Sub<Output = T>,
{
    let rhs = *rhs;
    unary(lhs, |a| a - rhs, lhs.data_type().clone())
}

/// Checked subtraction of a scalar T to a primitive array of type T. If the
/// result from the subtraction overflows, then the validity for that index
/// is changed to None
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::sub::checked_sub_scalar;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![None, Some(-100), None, Some(-100)]).to(DataType::Int8);
/// let result = checked_sub_scalar(&a, &100i8);
/// let expected = Primitive::<i8>::from(&vec![None, None, None, None]).to(DataType::Int8);
/// assert_eq!(result, expected);
/// ```
#[inline]
pub fn checked_sub_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> PrimitiveArray<T>
where
    T: NativeType + CheckedSub<Output = T> + Zero,
{
    let rhs = *rhs;
    let op = move |a: T| a.checked_sub(&rhs);

    unary_checked(lhs, op, lhs.data_type().clone())
}

/// Saturated subtraction of a scalar T to a primitive array of type T. If the
/// result from the sub is smaller than the possible number for this type, then
/// the result will be saturated
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::sub::saturating_sub_scalar;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(-100i8)]).to(DataType::Int8);
/// let result = saturating_sub_scalar(&a, &100i8);
/// let expected = Primitive::from(&vec![Some(-128i8)]).to(DataType::Int8);
/// assert_eq!(result, expected);
/// ```
#[inline]
pub fn saturating_sub_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> PrimitiveArray<T>
where
    T: NativeType + SaturatingSub<Output = T>,
{
    let rhs = *rhs;
    let op = move |a: T| a.saturating_sub(&rhs);

    unary(lhs, op, lhs.data_type().clone())
}

/// Overflowing subtraction of a scalar T to a primitive array of type T. If
/// the result from the sub is smaller than the possible number for this type,
/// then the result will be an array with overflowed values and a validity
/// array indicating the overflowing elements from the array
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::sub::overflowing_sub_scalar;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(1i8), Some(-100i8)]).to(DataType::Int8);
/// let (result, overflow) = overflowing_sub_scalar(&a, &100i8);
/// let expected = Primitive::from(&vec![Some(-99i8), Some(56i8)]).to(DataType::Int8);
/// assert_eq!(result, expected);
/// ```
#[inline]
pub fn overflowing_sub_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> (PrimitiveArray<T>, Bitmap)
where
    T: NativeType + OverflowingSub<Output = T>,
{
    let rhs = *rhs;
    let op = move |a: T| a.overflowing_sub(&rhs);

    unary_with_bitmap(lhs, op, lhs.data_type().clone())
}

// Implementation of ArraySub trait for PrimitiveArrays with a scalar
impl<T> ArraySub<T> for PrimitiveArray<T>
where
    T: NativeType + Sub<Output = T> + NotI128,
{
    type Output = Self;

    fn sub(&self, rhs: &T) -> Result<Self::Output> {
        Ok(sub_scalar(self, rhs))
    }
}

// Implementation of ArrayCheckedSub trait for PrimitiveArrays with a scalar
impl<T> ArrayCheckedSub<T> for PrimitiveArray<T>
where
    T: NativeType + CheckedSub<Output = T> + Zero + NotI128,
{
    type Output = Self;

    fn checked_sub(&self, rhs: &T) -> Result<Self::Output> {
        Ok(checked_sub_scalar(self, rhs))
    }
}

// Implementation of ArraySaturatingSub trait for PrimitiveArrays with a scalar
impl<T> ArraySaturatingSub<T> for PrimitiveArray<T>
where
    T: NativeType + SaturatingSub<Output = T> + NotI128,
{
    type Output = Self;

    fn saturating_sub(&self, rhs: &T) -> Result<Self::Output> {
        Ok(saturating_sub_scalar(self, rhs))
    }
}

// Implementation of ArraySaturatingSub trait for PrimitiveArrays with a scalar
impl<T> ArrayOverflowingSub<T> for PrimitiveArray<T>
where
    T: NativeType + OverflowingSub<Output = T> + NotI128,
{
    type Output = Self;

    fn overflowing_sub(&self, rhs: &T) -> Result<(Self::Output, Bitmap)> {
        Ok(overflowing_sub_scalar(self, rhs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Primitive;
    use crate::datatypes::DataType;

    #[test]
    fn test_sub_mismatched_length() {
        let a = Primitive::from_slice(vec![5, 6]).to(DataType::Int32);
        let b = Primitive::from_slice(vec![5]).to(DataType::Int32);
        sub(&a, &b)
            .err()
            .expect("should have failed due to different lengths");
    }

    #[test]
    fn test_sub() {
        let a = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
        let b = Primitive::from(&vec![Some(5), None, None, Some(6)]).to(DataType::Int32);
        let result = sub(&a, &b).unwrap();
        let expected = Primitive::from(&vec![None, None, None, Some(0)]).to(DataType::Int32);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.sub(&b).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    #[should_panic]
    fn test_sub_panic() {
        let a = Primitive::from(&vec![Some(-100i8)]).to(DataType::Int8);
        let b = Primitive::from(&vec![Some(100i8)]).to(DataType::Int8);
        let _ = sub(&a, &b);
    }

    #[test]
    fn test_sub_checked() {
        let a = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
        let b = Primitive::from(&vec![Some(5), None, None, Some(6)]).to(DataType::Int32);
        let result = checked_sub(&a, &b).unwrap();
        let expected = Primitive::from(&vec![None, None, None, Some(0)]).to(DataType::Int32);
        assert_eq!(result, expected);

        let a = Primitive::from(&vec![Some(100i8), Some(-100i8), Some(100i8)]).to(DataType::Int8);
        let b = Primitive::from(&vec![Some(1i8), Some(100i8), Some(0i8)]).to(DataType::Int8);
        let result = checked_sub(&a, &b).unwrap();
        let expected = Primitive::from(&vec![Some(99i8), None, Some(100i8)]).to(DataType::Int8);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.checked_sub(&b).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_sub_saturating() {
        let a = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
        let b = Primitive::from(&vec![Some(5), None, None, Some(6)]).to(DataType::Int32);
        let result = saturating_sub(&a, &b).unwrap();
        let expected = Primitive::from(&vec![None, None, None, Some(0)]).to(DataType::Int32);
        assert_eq!(result, expected);

        let a = Primitive::from(&vec![Some(-100i8)]).to(DataType::Int8);
        let b = Primitive::from(&vec![Some(100i8)]).to(DataType::Int8);
        let result = saturating_sub(&a, &b).unwrap();
        let expected = Primitive::from(&vec![Some(-128)]).to(DataType::Int8);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.saturating_sub(&b).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_sub_overflowing() {
        let a = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
        let b = Primitive::from(&vec![Some(5), None, None, Some(6)]).to(DataType::Int32);
        let (result, overflow) = overflowing_sub(&a, &b).unwrap();
        let expected = Primitive::from(&vec![None, None, None, Some(0)]).to(DataType::Int32);
        assert_eq!(result, expected);
        assert_eq!(overflow.as_slice()[0], 0b0000);

        let a = Primitive::from(&vec![Some(1i8), Some(-100i8)]).to(DataType::Int8);
        let b = Primitive::from(&vec![Some(1i8), Some(100i8)]).to(DataType::Int8);
        let (result, overflow) = overflowing_sub(&a, &b).unwrap();
        let expected = Primitive::from(&vec![Some(0i8), Some(56i8)]).to(DataType::Int8);
        assert_eq!(result, expected);
        assert_eq!(overflow.as_slice()[0], 0b10);

        // Trait testing
        let (result, overflow) = a.overflowing_sub(&b).unwrap();
        assert_eq!(result, expected);
        assert_eq!(overflow.as_slice()[0], 0b10);
    }

    #[test]
    fn test_sub_scalar() {
        let a = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
        let result = sub_scalar(&a, &1i32);
        let expected = Primitive::from(&vec![None, Some(5), None, Some(5)]).to(DataType::Int32);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.sub(&1i32).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_sub_scalar_checked() {
        let a = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
        let result = checked_sub_scalar(&a, &1i32);
        let expected = Primitive::from(&vec![None, Some(5), None, Some(5)]).to(DataType::Int32);
        assert_eq!(result, expected);

        let a = Primitive::from(&vec![None, Some(-100), None, Some(-100)]).to(DataType::Int8);
        let result = checked_sub_scalar(&a, &100i8);
        let expected = Primitive::<i8>::from(&vec![None, None, None, None]).to(DataType::Int8);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.checked_sub(&100i8).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_sub_scalar_saturating() {
        let a = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
        let result = saturating_sub_scalar(&a, &1i32);
        let expected = Primitive::from(&vec![None, Some(5), None, Some(5)]).to(DataType::Int32);
        assert_eq!(result, expected);

        let a = Primitive::from(&vec![Some(-100i8)]).to(DataType::Int8);
        let result = saturating_sub_scalar(&a, &100i8);
        let expected = Primitive::from(&vec![Some(-128)]).to(DataType::Int8);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.saturating_sub(&100i8).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_sub_scalar_overflowing() {
        let a = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
        let (result, overflow) = overflowing_sub_scalar(&a, &1i32);
        let expected = Primitive::from(&vec![None, Some(5), None, Some(5)]).to(DataType::Int32);
        assert_eq!(result, expected);
        assert_eq!(overflow.as_slice()[0], 0b0000);

        let a = Primitive::from(&vec![Some(1i8), Some(-100i8)]).to(DataType::Int8);
        let (result, overflow) = overflowing_sub_scalar(&a, &100i8);
        let expected = Primitive::from(&vec![Some(-99i8), Some(56i8)]).to(DataType::Int8);
        assert_eq!(result, expected);
        assert_eq!(overflow.as_slice()[0], 0b10);

        // Trait testing
        let (result, overflow) = a.overflowing_sub(&100i8).unwrap();
        assert_eq!(result, expected);
        assert_eq!(overflow.as_slice()[0], 0b10);
    }
}
