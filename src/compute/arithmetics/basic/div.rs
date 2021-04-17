use std::ops::Div;

use crate::{
    array::{Array, PrimitiveArray},
    buffer::Buffer,
    compute::arity::unary,
    error::{ArrowError, Result},
    types::NativeType,
};

use num::Zero;

use crate::compute::utils::combine_validities;

/// Divide two arrays.
///
/// # Errors
///
/// This function errors iff:
/// * the arrays have different lengths
/// * a division by zero is found
pub fn div<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: NativeType,
    T: Div<Output = T> + Zero,
{
    if lhs.len() != rhs.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Cannot perform math operation on arrays of different length".to_string(),
        ));
    }

    let validity = combine_validities(lhs.validity(), rhs.validity());

    let values = if let Some(b) = &validity {
        // there are nulls. Division by zero on them should be ignored
        let values =
            b.iter()
                .zip(lhs.values().iter().zip(rhs.values()))
                .map(|(is_valid, (lhs, rhs))| {
                    if is_valid {
                        if rhs.is_zero() {
                            Err(ArrowError::InvalidArgumentError(
                                "There is a zero in the division, causing a division by zero"
                                    .to_string(),
                            ))
                        } else {
                            Ok(*lhs / *rhs)
                        }
                    } else {
                        Ok(T::default())
                    }
                });
        unsafe { Buffer::try_from_trusted_len_iter(values) }
    } else {
        // no value is null
        let values = lhs.values().iter().zip(rhs.values()).map(|(lhs, rhs)| {
            if rhs.is_zero() {
                Err(ArrowError::InvalidArgumentError(
                    "There is a zero in the division, causing a division by zero".to_string(),
                ))
            } else {
                Ok(*lhs / *rhs)
            }
        });
        unsafe { Buffer::try_from_trusted_len_iter(values) }
    }?;

    Ok(PrimitiveArray::<T>::from_data(
        lhs.data_type().clone(),
        values,
        validity,
    ))
}

/// Divide an array by a constant
pub fn div_scalar<T>(array: &PrimitiveArray<T>, divisor: &T) -> Result<PrimitiveArray<T>>
where
    T: NativeType,
    T: Div<Output = T> + Zero,
{
    if divisor.is_zero() {
        return Err(ArrowError::InvalidArgumentError(
            "The divisor cannot be zero".to_string(),
        ));
    }
    let divisor = *divisor;
    Ok(unary(array, |x| x / divisor, array.data_type()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Primitive;
    use crate::datatypes::DataType;

    #[test]
    fn test_divide() {
        let a = Primitive::from(&vec![None, Some(6), None, Some(6)]).to(DataType::Int32);
        let b = Primitive::from(&vec![Some(5), None, None, Some(6)]).to(DataType::Int32);
        let result = div(&a, &b).unwrap();
        let expected = Primitive::from(&vec![None, None, None, Some(1)]).to(DataType::Int32);
        assert_eq!(result, expected)
    }

    #[test]
    fn test_divide_scalar() {
        let a = Primitive::from(&vec![None, Some(6)]).to(DataType::Int32);
        let b = 3i32;
        let result = div_scalar(&a, &b).unwrap();
        let expected = Primitive::from(&vec![None, Some(2)]).to(DataType::Int32);
        assert_eq!(result, expected)
    }

    #[test]
    fn test_divide_by_zero() {
        let a = Primitive::from(&vec![Some(6)]).to(DataType::Int32);
        let b = Primitive::from(&vec![Some(0)]).to(DataType::Int32);
        assert_eq!(div(&a, &b).is_err(), true);
    }

    #[test]
    fn test_divide_by_zero_on_null() {
        let a = Primitive::from(&vec![None]).to(DataType::Int32);
        let b = Primitive::from(&vec![Some(0)]).to(DataType::Int32);
        assert_eq!(div(&a, &b).is_err(), false);
    }
}
