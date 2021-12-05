//! Definition of basic pow operations with primitive arrays
use num_traits::{checked_pow, CheckedMul, One, Pow, Zero};

use crate::{
    array::{Array, PrimitiveArray},
    compute::arity::unary_checked,
    scalar::PrimitiveScalar,
};

use super::binary_scalar;
use super::NativeArithmetics;

/// Raises an array of primitives to the power of exponent. Panics if one of
/// the values values overflows.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::powf_scalar;
/// use arrow2::array::PrimitiveArray;
/// use arrow2::array::PrimitiveScalar;
///
/// let a = PrimitiveArray::<f32>::from(&[Some(2f32), None]);
/// let b = PrimitiveScalar::<f32>::from(Some(2.0f32));
/// let actual = powf_scalar(&a, &b);
/// let expected = Float32Array::from(&[Some(4f32), None]);
/// assert_eq!(expected, actual);
/// ```
pub fn powf_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveScalar<T>) -> PrimitiveArray<T>
where
    T: NativeArithmetics + Pow<T, Output = T>,
{
    binary_scalar(lhs, rhs, |a, b| a.pow(b))
}

/// Checked operation of raising an array of primitives to the power of
/// exponent. If the result from the multiplications overflows, the validity
/// for that index is changed returned.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::checked_powf_scalar;
/// use arrow2::array::Int8Array;
///
/// let a = Int8Array::from(&[Some(1i8), None, Some(7i8)]);
/// let actual = checked_powf_scalar(&a, 8usize);
/// let expected = Int8Array::from(&[Some(1i8), None, None]);
/// assert_eq!(expected, actual);
/// ```
pub fn checked_powf_scalar<T>(array: &PrimitiveArray<T>, exponent: usize) -> PrimitiveArray<T>
where
    T: NativeArithmetics + Zero + One + CheckedMul,
{
    let op = move |a: T| checked_pow(a, exponent);

    unary_checked(array, op, array.data_type().clone())
}
