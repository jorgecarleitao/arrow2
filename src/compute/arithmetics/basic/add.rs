//! Definition of basic add operations with primitive arrays
use std::ops::Add;

use num::{
    traits::{ops::overflowing::OverflowingAdd, SaturatingAdd},
    CheckedAdd, Zero,
};

use crate::{
    array::{Array, PrimitiveArray},
    bitmap::Bitmap,
    compute::{
        arithmetics::{
            ArrayAdd, ArrayCheckedAdd, ArrayOverflowingAdd, ArraySaturatingAdd, NotI128,
        },
        arity::{
            binary, binary_checked, binary_with_bitmap, unary, unary_checked, unary_with_bitmap,
        },
    },
    error::{ArrowError, Result},
    types::NativeType,
};

/// Adds two primitive arrays with the same type.
/// Panics if the sum of one pair of values overflows.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::add::add;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
/// let b = Primitive::from(&vec![Some(5), None, None, Some(6)]).to(DataType::Int32);
/// let result = add(&a, &b).unwrap();
/// let expected = Primitive::from(&vec![None, None, None, Some(12)]).to(DataType::Int32);
/// assert_eq!(result, expected)
/// ```
pub fn add<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: NativeType + Add<Output = T>,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }

    binary(lhs, rhs, lhs.data_type().clone(), |a, b| a + b)
}

/// Checked addition of two primitive arrays. If the result from the sum
/// overflows, the validity for that index is changed to None
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::add::checked_add;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(100i8), Some(100i8), Some(100i8)]).to(DataType::Int8);
/// let b = Primitive::from(&vec![Some(0i8), Some(100i8), Some(0i8)]).to(DataType::Int8);
/// let result = checked_add(&a, &b).unwrap();
/// let expected = Primitive::from(&vec![Some(100i8), None, Some(100i8)]).to(DataType::Int8);
/// assert_eq!(result, expected);
/// ```
pub fn checked_add<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: NativeType + CheckedAdd<Output = T> + Zero,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }

    let op = move |a: T, b: T| a.checked_add(&b);

    binary_checked(lhs, rhs, lhs.data_type().clone(), op)
}

/// Saturating addition of two primitive arrays. If the result from the sum is
/// larger than the possible number for this type, the result for the operation
/// will be the saturated value.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::add::saturating_add;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(100i8)]).to(DataType::Int8);
/// let b = Primitive::from(&vec![Some(100i8)]).to(DataType::Int8);
/// let result = saturating_add(&a, &b).unwrap();
/// let expected = Primitive::from(&vec![Some(127)]).to(DataType::Int8);
/// assert_eq!(result, expected);
/// ```
pub fn saturating_add<T>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: NativeType + SaturatingAdd<Output = T>,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }

    let op = move |a: T, b: T| a.saturating_add(&b);

    binary(lhs, rhs, lhs.data_type().clone(), op)
}

/// Overflowing addition of two primitive arrays. If the result from the sum is
/// larger than the possible number for this type, the result for the operation
/// will be an array with overflowed values and a  validity array indicating
/// the overflowing elements from the array.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::add::overflowing_add;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(1i8), Some(100i8)]).to(DataType::Int8);
/// let b = Primitive::from(&vec![Some(1i8), Some(100i8)]).to(DataType::Int8);
/// let (result, overflow) = overflowing_add(&a, &b).unwrap();
/// let expected = Primitive::from(&vec![Some(2i8), Some(-56i8)]).to(DataType::Int8);
/// assert_eq!(result, expected);
/// ```
pub fn overflowing_add<T>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
) -> Result<(PrimitiveArray<T>, Bitmap)>
where
    T: NativeType + OverflowingAdd<Output = T>,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }

    let op = move |a: T, b: T| a.overflowing_add(&b);

    binary_with_bitmap(lhs, rhs, lhs.data_type().clone(), op)
}

// Implementation of ArrayAdd trait for PrimitiveArrays
impl<T> ArrayAdd<PrimitiveArray<T>> for PrimitiveArray<T>
where
    T: NativeType + Add<Output = T> + NotI128,
{
    type Output = Self;

    fn add(&self, rhs: &PrimitiveArray<T>) -> Result<Self::Output> {
        add(self, rhs)
    }
}

// Implementation of ArrayCheckedAdd trait for PrimitiveArrays
impl<T> ArrayCheckedAdd<PrimitiveArray<T>> for PrimitiveArray<T>
where
    T: NativeType + CheckedAdd<Output = T> + Zero + NotI128,
{
    type Output = Self;

    fn checked_add(&self, rhs: &PrimitiveArray<T>) -> Result<Self::Output> {
        checked_add(self, rhs)
    }
}

// Implementation of ArraySaturatingAdd trait for PrimitiveArrays
impl<T> ArraySaturatingAdd<PrimitiveArray<T>> for PrimitiveArray<T>
where
    T: NativeType + SaturatingAdd<Output = T> + NotI128,
{
    type Output = Self;

    fn saturating_add(&self, rhs: &PrimitiveArray<T>) -> Result<Self::Output> {
        saturating_add(self, rhs)
    }
}

// Implementation of ArraySaturatingAdd trait for PrimitiveArrays
impl<T> ArrayOverflowingAdd<PrimitiveArray<T>> for PrimitiveArray<T>
where
    T: NativeType + OverflowingAdd<Output = T> + NotI128,
{
    type Output = Self;

    fn overflowing_add(&self, rhs: &PrimitiveArray<T>) -> Result<(Self::Output, Bitmap)> {
        overflowing_add(self, rhs)
    }
}

/// Adds a scalar T to a primitive array of type T.
/// Panics if the sum of the values overflows.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::add::add_scalar;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
/// let result = add_scalar(&a, &1i32);
/// let expected = Primitive::from(&vec![None, Some(7), None, Some(7)]).to(DataType::Int32);
/// assert_eq!(result, expected)
/// ```
pub fn add_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> PrimitiveArray<T>
where
    T: NativeType + Add<Output = T>,
{
    let rhs = *rhs;
    unary(lhs, |a| a + rhs, lhs.data_type().clone())
}

/// Checked addition of a scalar T to a primitive array of type T. If the
/// result from the sum overflows then the validity index for that value is
/// changed to None
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::add::checked_add_scalar;
/// use arrow2::array::Int8Array;
///
/// let a = Int8Array::from(&[None, Some(100), None, Some(100)]);
/// let result = checked_add_scalar(&a, &100i8);
/// let expected = Int8Array::from(&[None, None, None, None]);
/// assert_eq!(result, expected);
/// ```
pub fn checked_add_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> PrimitiveArray<T>
where
    T: NativeType + CheckedAdd<Output = T> + Zero,
{
    let rhs = *rhs;
    let op = move |a: T| a.checked_add(&rhs);

    unary_checked(lhs, op, lhs.data_type().clone())
}

/// Saturated addition of a scalar T to a primitive array of type T. If the
/// result from the sum is larger than the possible number for this type, then
/// the result will be saturated
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::add::saturating_add_scalar;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(100i8)]).to(DataType::Int8);
/// let result = saturating_add_scalar(&a, &100i8);
/// let expected = Primitive::from(&vec![Some(127)]).to(DataType::Int8);
/// assert_eq!(result, expected);
/// ```
pub fn saturating_add_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> PrimitiveArray<T>
where
    T: NativeType + SaturatingAdd<Output = T>,
{
    let rhs = *rhs;
    let op = move |a: T| a.saturating_add(&rhs);

    unary(lhs, op, lhs.data_type().clone())
}

/// Overflowing addition of a scalar T to a primitive array of type T. If the
/// result from the sum is larger than the possible number for this type, then
/// the result will be an array with overflowed values and a validity array
/// indicating the overflowing elements from the array
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::add::overflowing_add_scalar;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(1i8), Some(100i8)]).to(DataType::Int8);
/// let (result, overflow) = overflowing_add_scalar(&a, &100i8);
/// let expected = Primitive::from(&vec![Some(101i8), Some(-56i8)]).to(DataType::Int8);
/// assert_eq!(result, expected);
/// ```
pub fn overflowing_add_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> (PrimitiveArray<T>, Bitmap)
where
    T: NativeType + OverflowingAdd<Output = T>,
{
    let rhs = *rhs;
    let op = move |a: T| a.overflowing_add(&rhs);

    unary_with_bitmap(lhs, op, lhs.data_type().clone())
}

// Implementation of ArrayAdd trait for PrimitiveArrays with a scalar
impl<T> ArrayAdd<T> for PrimitiveArray<T>
where
    T: NativeType + Add<Output = T> + NotI128,
{
    type Output = Self;

    fn add(&self, rhs: &T) -> Result<Self::Output> {
        Ok(add_scalar(self, rhs))
    }
}

// Implementation of ArrayCheckedAdd trait for PrimitiveArrays with a scalar
impl<T> ArrayCheckedAdd<T> for PrimitiveArray<T>
where
    T: NativeType + CheckedAdd<Output = T> + Zero + NotI128,
{
    type Output = Self;

    fn checked_add(&self, rhs: &T) -> Result<Self::Output> {
        Ok(checked_add_scalar(self, rhs))
    }
}

// Implementation of ArraySaturatingAdd trait for PrimitiveArrays with a scalar
impl<T> ArraySaturatingAdd<T> for PrimitiveArray<T>
where
    T: NativeType + SaturatingAdd<Output = T> + NotI128,
{
    type Output = Self;

    fn saturating_add(&self, rhs: &T) -> Result<Self::Output> {
        Ok(saturating_add_scalar(self, rhs))
    }
}

// Implementation of ArraySaturatingAdd trait for PrimitiveArrays with a scalar
impl<T> ArrayOverflowingAdd<T> for PrimitiveArray<T>
where
    T: NativeType + OverflowingAdd<Output = T> + NotI128,
{
    type Output = Self;

    fn overflowing_add(&self, rhs: &T) -> Result<(Self::Output, Bitmap)> {
        Ok(overflowing_add_scalar(self, rhs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::*;

    #[test]
    fn test_add_mismatched_length() {
        let a = Int32Array::from_slice(&[5, 6]);
        let b = Int32Array::from_slice(&[5]);
        add(&a, &b)
            .err()
            .expect("should have failed due to different lengths");
    }

    #[test]
    fn test_add() {
        let a = Int32Array::from(&[None, Some(6), None, Some(6)]);
        let b = Int32Array::from(&[Some(5), None, None, Some(6)]);
        let result = add(&a, &b).unwrap();
        let expected = Int32Array::from(&[None, None, None, Some(12)]);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.add(&b).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    #[should_panic]
    fn test_add_panic() {
        let a = Int8Array::from(&[Some(100i8)]);
        let b = Int8Array::from(&[Some(100i8)]);
        let _ = add(&a, &b);
    }

    #[test]
    fn test_add_checked() {
        let a = Int32Array::from(&[None, Some(6), None, Some(6)]);
        let b = Int32Array::from(&[Some(5), None, None, Some(6)]);
        let result = checked_add(&a, &b).unwrap();
        let expected = Int32Array::from(&[None, None, None, Some(12)]);
        assert_eq!(result, expected);

        let a = Int8Array::from(&[Some(100i8), Some(100i8), Some(100i8)]);
        let b = Int8Array::from(&[Some(0i8), Some(100i8), Some(0i8)]);
        let result = checked_add(&a, &b).unwrap();
        let expected = Int8Array::from(&[Some(100i8), None, Some(100i8)]);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.checked_add(&b).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_add_saturating() {
        let a = Int32Array::from(&[None, Some(6), None, Some(6)]);
        let b = Int32Array::from(&[Some(5), None, None, Some(6)]);
        let result = saturating_add(&a, &b).unwrap();
        let expected = Int32Array::from(&[None, None, None, Some(12)]);
        assert_eq!(result, expected);

        let a = Int8Array::from(&[Some(100i8)]);
        let b = Int8Array::from(&[Some(100i8)]);
        let result = saturating_add(&a, &b).unwrap();
        let expected = Int8Array::from(&[Some(127)]);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.saturating_add(&b).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_add_overflowing() {
        let a = Int32Array::from(&[None, Some(6), None, Some(6)]);
        let b = Int32Array::from(&[Some(5), None, None, Some(6)]);
        let (result, overflow) = overflowing_add(&a, &b).unwrap();
        let expected = Int32Array::from(&[None, None, None, Some(12)]);
        assert_eq!(result, expected);
        assert_eq!(overflow.as_slice()[0], 0b0000);

        let a = Int8Array::from(&[Some(1i8), Some(100i8)]);
        let b = Int8Array::from(&[Some(1i8), Some(100i8)]);
        let (result, overflow) = overflowing_add(&a, &b).unwrap();
        let expected = Int8Array::from(&[Some(2i8), Some(-56i8)]);
        assert_eq!(result, expected);
        assert_eq!(overflow.as_slice()[0], 0b10);

        // Trait testing
        let (result, overflow) = a.overflowing_add(&b).unwrap();
        assert_eq!(result, expected);
        assert_eq!(overflow.as_slice()[0], 0b10);
    }

    #[test]
    fn test_add_scalar() {
        let a = Int32Array::from(&[None, Some(6), None, Some(6)]);
        let result = add_scalar(&a, &1i32);
        let expected = Int32Array::from(&[None, Some(7), None, Some(7)]);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.add(&1i32).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_add_scalar_checked() {
        let a = Int32Array::from(&[None, Some(6), None, Some(6)]);
        let result = checked_add_scalar(&a, &1i32);
        let expected = Int32Array::from(&[None, Some(7), None, Some(7)]);
        assert_eq!(result, expected);

        let a = Int8Array::from(&[None, Some(100), None, Some(100)]);
        let result = checked_add_scalar(&a, &100i8);
        let expected = Int8Array::from(&[None, None, None, None]);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.checked_add(&100i8).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_add_scalar_saturating() {
        let a = Int32Array::from(&[None, Some(6), None, Some(6)]);
        let result = saturating_add_scalar(&a, &1i32);
        let expected = Int32Array::from(&[None, Some(7), None, Some(7)]);
        assert_eq!(result, expected);

        let a = Int8Array::from(&[Some(100i8)]);
        let result = saturating_add_scalar(&a, &100i8);
        let expected = Int8Array::from(&[Some(127)]);
        assert_eq!(result, expected);

        // Trait testing
        let result = a.saturating_add(&100i8).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_add_scalar_overflowing() {
        let a = Int32Array::from(&vec![None, Some(6), None, Some(6)]);
        let (result, overflow) = overflowing_add_scalar(&a, &1i32);
        let expected = Int32Array::from(&vec![None, Some(7), None, Some(7)]);
        assert_eq!(result, expected);
        assert_eq!(overflow.as_slice()[0], 0b0000);

        let a = Int8Array::from(&vec![Some(1i8), Some(100i8)]);
        let (result, overflow) = overflowing_add_scalar(&a, &100i8);
        let expected = Int8Array::from(&vec![Some(101i8), Some(-56i8)]);
        assert_eq!(result, expected);
        assert_eq!(overflow.as_slice()[0], 0b10);

        // Trait testing
        let (result, overflow) = a.overflowing_add(&100i8).unwrap();
        assert_eq!(result, expected);
        assert_eq!(overflow.as_slice()[0], 0b10);
    }
}
