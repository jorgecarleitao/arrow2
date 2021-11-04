use std::ops::Rem;

use num_traits::{CheckedRem, NumCast};

use crate::compute::arithmetics::basic::check_same_type;
use crate::datatypes::DataType;
use crate::{
    array::{Array, PrimitiveArray},
    compute::{
        arithmetics::{ArrayCheckedRem, ArrayRem, NativeArithmetics},
        arity::{binary, binary_checked, unary, unary_checked},
    },
    error::Result,
    types::NativeType,
};
use strength_reduce::{
    StrengthReducedU16, StrengthReducedU32, StrengthReducedU64, StrengthReducedU8,
};

/// Remainder of two primitive arrays with the same type.
/// Panics if the divisor is zero of one pair of values overflows.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::rem;
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
    check_same_type(lhs, rhs)?;

    binary(lhs, rhs, lhs.data_type().clone(), |a, b| a % b)
}

/// Checked remainder of two primitive arrays. If the result from the remainder
/// overflows, the result for the operation will change the validity array
/// making this operation None
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::checked_rem;
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
    T: NativeType + CheckedRem<Output = T>,
{
    check_same_type(lhs, rhs)?;

    let op = move |a: T, b: T| a.checked_rem(&b);

    binary_checked(lhs, rhs, lhs.data_type().clone(), op)
}

impl<T> ArrayRem<PrimitiveArray<T>> for PrimitiveArray<T>
where
    T: NativeArithmetics + Rem<Output = T>,
{
    type Output = Self;

    fn rem(&self, rhs: &PrimitiveArray<T>) -> Result<Self::Output> {
        rem(self, rhs)
    }
}

impl<T> ArrayCheckedRem<PrimitiveArray<T>> for PrimitiveArray<T>
where
    T: NativeArithmetics + CheckedRem<Output = T>,
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
/// use arrow2::compute::arithmetics::basic::rem_scalar;
/// use arrow2::array::Int32Array;
///
/// let a = Int32Array::from(&[None, Some(6), None, Some(7)]);
/// let result = rem_scalar(&a, &2i32);
/// let expected = Int32Array::from(&[None, Some(0), None, Some(1)]);
/// assert_eq!(result, expected)
/// ```
pub fn rem_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> PrimitiveArray<T>
where
    T: NativeType + Rem<Output = T> + NumCast,
{
    let rhs = *rhs;

    match T::DATA_TYPE {
        DataType::UInt64 => {
            let lhs = lhs.as_any().downcast_ref::<PrimitiveArray<u64>>().unwrap();
            let rhs = rhs.to_u64().unwrap();

            let reduced_rem = StrengthReducedU64::new(rhs);
            // Safety: we just proved that `lhs` is `PrimitiveArray<u64>` which means that
            // T = u64
            unsafe {
                std::mem::transmute::<PrimitiveArray<u64>, PrimitiveArray<T>>(unary(
                    lhs,
                    |a| a % reduced_rem,
                    lhs.data_type().clone(),
                ))
            }
        }
        DataType::UInt32 => {
            let lhs = lhs.as_any().downcast_ref::<PrimitiveArray<u32>>().unwrap();
            let rhs = rhs.to_u32().unwrap();

            let reduced_rem = StrengthReducedU32::new(rhs);
            // Safety: we just proved that `lhs` is `PrimitiveArray<u32>` which means that
            // T = u32
            unsafe {
                std::mem::transmute::<PrimitiveArray<u32>, PrimitiveArray<T>>(unary(
                    lhs,
                    |a| a % reduced_rem,
                    lhs.data_type().clone(),
                ))
            }
        }
        DataType::UInt16 => {
            let lhs = lhs.as_any().downcast_ref::<PrimitiveArray<u16>>().unwrap();
            let rhs = rhs.to_u16().unwrap();

            let reduced_rem = StrengthReducedU16::new(rhs);
            // Safety: we just proved that `lhs` is `PrimitiveArray<u16>` which means that
            // T = u16
            unsafe {
                std::mem::transmute::<PrimitiveArray<u16>, PrimitiveArray<T>>(unary(
                    lhs,
                    |a| a % reduced_rem,
                    lhs.data_type().clone(),
                ))
            }
        }
        DataType::UInt8 => {
            let lhs = lhs.as_any().downcast_ref::<PrimitiveArray<u8>>().unwrap();
            let rhs = rhs.to_u8().unwrap();

            let reduced_rem = StrengthReducedU8::new(rhs);
            // Safety: we just proved that `lhs` is `PrimitiveArray<u8>` which means that
            // T = u8
            unsafe {
                std::mem::transmute::<PrimitiveArray<u8>, PrimitiveArray<T>>(unary(
                    lhs,
                    |a| a % reduced_rem,
                    lhs.data_type().clone(),
                ))
            }
        }
        _ => unary(lhs, |a| a % rhs, lhs.data_type().clone()),
    }
}

/// Checked remainder of a primitive array of type T by a scalar T. If the
/// divisor is zero then the validity array is changed to None.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::checked_rem_scalar;
/// use arrow2::array::Int8Array;
///
/// let a = Int8Array::from(&[Some(-100i8)]);
/// let result = checked_rem_scalar(&a, &100i8);
/// let expected = Int8Array::from(&[Some(0i8)]);
/// assert_eq!(result, expected);
/// ```
pub fn checked_rem_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T) -> PrimitiveArray<T>
where
    T: NativeType + CheckedRem<Output = T>,
{
    let rhs = *rhs;
    let op = move |a: T| a.checked_rem(&rhs);

    unary_checked(lhs, op, lhs.data_type().clone())
}

impl<T> ArrayRem<T> for PrimitiveArray<T>
where
    T: NativeArithmetics + Rem<Output = T> + NumCast,
{
    type Output = Self;

    fn rem(&self, rhs: &T) -> Result<Self::Output> {
        Ok(rem_scalar(self, rhs))
    }
}

impl<T> ArrayCheckedRem<T> for PrimitiveArray<T>
where
    T: NativeArithmetics + CheckedRem<Output = T>,
{
    type Output = Self;

    fn checked_rem(&self, rhs: &T) -> Result<Self::Output> {
        Ok(checked_rem_scalar(self, rhs))
    }
}
