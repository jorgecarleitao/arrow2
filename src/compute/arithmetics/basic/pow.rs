//! Definition of basic pow operations with primitive arrays
use num::{checked_pow, traits::Pow, CheckedMul, One, Zero};

use crate::{
    array::{Array, PrimitiveArray},
    compute::arity::{unary, unary_checked},
    types::NativeType,
};

/// Raises an array of primitives to the power of exponent. Panics if one of
/// the values values overflows.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::pow::powf_scalar;
/// use arrow2::array::Float32Array;
///
/// let a = Float32Array::from(&[Some(2f32), None]);
/// let actual = powf_scalar(&a, 2.0);
/// let expected = Float32Array::from(&[Some(4f32), None]);
/// assert_eq!(expected, actual);
/// ```
pub fn powf_scalar<T>(array: &PrimitiveArray<T>, exponent: T) -> PrimitiveArray<T>
where
    T: NativeType + Pow<T, Output = T>,
{
    unary(array, |x| x.pow(exponent), array.data_type().clone())
}

/// Checked operation of raising an array of primitives to the power of
/// exponent. If the result from the multiplications overflows, the validity
/// for that index is changed returned.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::basic::pow::checked_powf_scalar;
/// use arrow2::array::Int8Array;
///
/// let a = Int8Array::from(&[Some(1i8), None, Some(7i8)]);
/// let actual = checked_powf_scalar(&a, 8usize);
/// let expected = Int8Array::from(&[Some(1i8), None, None]);
/// assert_eq!(expected, actual);
/// ```
pub fn checked_powf_scalar<T>(array: &PrimitiveArray<T>, exponent: usize) -> PrimitiveArray<T>
where
    T: NativeType + Zero + One + CheckedMul,
{
    let op = move |a: T| checked_pow(a, exponent);

    unary_checked(array, op, array.data_type().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{Float32Array, Int8Array};

    #[test]
    fn test_raise_power_scalar() {
        let a = Float32Array::from(&[Some(2f32), None]);
        let actual = powf_scalar(&a, 2.0);
        let expected = Float32Array::from(&[Some(4f32), None]);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_raise_power_scalar_checked() {
        let a = Int8Array::from(&[Some(1i8), None, Some(7i8)]);
        let actual = checked_powf_scalar(&a, 8usize);
        let expected = Int8Array::from(&[Some(1i8), None, None]);
        assert_eq!(expected, actual);
    }
}
