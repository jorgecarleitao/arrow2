//! Defines kernels suitable to perform operations to primitive arrays.

use super::utils::combine_validities;
use crate::error::{ArrowError, Result};

use crate::buffer::Buffer;
use crate::types::NativeType;
use crate::{
    array::{Array, PrimitiveArray},
    datatypes::DataType,
};

/// Applies an unary and infallible function to a primitive array. This is the
/// fastest way to perform an operation on a primitive array when the benefits
/// of a vectorized operation outweighs the cost of branching nulls and
/// non-nulls.
///
/// # Implementation
/// This will apply the function for all values, including those on null slots.
/// This implies that the operation must be infallible for any value of the
/// corresponding type or this function may panic.
#[inline]
pub fn unary<I, F, O>(array: &PrimitiveArray<I>, op: F, data_type: &DataType) -> PrimitiveArray<O>
where
    I: NativeType,
    O: NativeType,
    F: Fn(I) -> O,
{
    let values = array.values().iter().map(|v| op(*v));
    // JUSTIFICATION
    //  Benefit
    //      ~60% speedup
    //  Soundness
    //      `values` is an iterator with a known size because arrays are sized.
    let values = unsafe { Buffer::from_trusted_len_iter(values) };

    PrimitiveArray::<O>::from_data(data_type.clone(), values, array.validity().clone())
}

/// Applies a binary operations to two primitive arrays. This is the fastest
/// way to perform an operation on two primitive array when the benefits of a
/// vectorized operation outweighs the cost of branching nulls and non-nulls.
///
/// # Implementation
/// This will apply the function for all values, including those on null slots.
/// This implies that the operation must be infallible for any value of the
/// corresponding type
#[inline]
pub fn binary<T, F>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
    op: F,
) -> Result<PrimitiveArray<T>>
where
    T: NativeType,
    F: Fn(T, T) -> T,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }
    if lhs.len() != rhs.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same length".to_string(),
        ));
    }

    let validity = combine_validities(lhs.validity(), rhs.validity());

    let values = lhs
        .values()
        .iter()
        .zip(rhs.values().iter())
        .map(|(l, r)| op(*l, *r));
    // JUSTIFICATION
    //  Benefit
    //      ~60% speedup
    //  Soundness
    //      `values` is an iterator with a known size.
    let values = unsafe { Buffer::from_trusted_len_iter(values) };

    Ok(PrimitiveArray::<T>::from_data(
        lhs.data_type().clone(),
        values,
        validity,
    ))
}

/// Applies a checked binary function to a pair of primitive arrays. The op
/// function has to return an Arrow Result to be used with this function. The
/// result from this function can be used to check if there was an
/// ArrowError::ArithmeticError.
pub fn try_binary<T, F>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
    op: F,
) -> Result<PrimitiveArray<T>>
where
    T: NativeType,
    F: Fn(T, T) -> Result<T>,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }
    if lhs.len() != rhs.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same length".to_string(),
        ));
    }

    let validity = combine_validities(lhs.validity(), rhs.validity());

    let values = lhs
        .values()
        .iter()
        .zip(rhs.values().iter())
        .map(|(l, r)| op(*l, *r));

    // JUSTIFICATION
    //  Benefit
    //      ~60% speedup
    //  Soundness
    //      `values` is an iterator with a known size.
    let values = unsafe { Buffer::try_from_trusted_len_iter(values) }?;

    Ok(PrimitiveArray::<T>::from_data(
        lhs.data_type().clone(),
        values,
        validity,
    ))
}
