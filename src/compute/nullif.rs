use crate::array::PrimitiveArray;
use crate::compute::comparison::primitive_compare_values_op;
use crate::error::{ArrowError, Result};
use crate::{array::Array, types::NativeType};

use super::utils::combine_validities;

/// Returns an array whose validity is null iff `lhs == rhs` or `lhs` is null.
/// This has the same semantics as postgres.
/// # Example
/// ```rust
/// # use arrow2::array::Int32Array;
/// # use arrow2::datatypes::DataType;
/// # use arrow2::error::Result;
/// # use arrow2::compute::nullif::nullif_primitive;
/// # fn main() -> Result<()> {
/// let lhs = Int32Array::from(&[None, None, Some(1), Some(1), Some(1)]);
/// let rhs = Int32Array::from(&[None, Some(1), None, Some(1), Some(0)]);
/// let result = nullif_primitive(&lhs, &rhs)?;
///
/// let expected = Int32Array::from(&[None, None, Some(1), None, Some(1)]);
///
/// assert_eq!(expected, result);
/// Ok(())
/// # }
/// ```
/// # Errors
/// This function errors iff
/// * The arguments do not have the same logical type
/// * The arguments do not have the same length
pub fn nullif_primitive<T: NativeType>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>> {
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }

    let equal = primitive_compare_values_op(lhs.values(), rhs.values(), |lhs, rhs| lhs != rhs);
    let equal = equal.into();

    let validity = combine_validities(lhs.validity(), &equal);

    Ok(PrimitiveArray::<T>::from_data(
        lhs.data_type().clone(),
        lhs.values_buffer().clone(),
        validity,
    ))
}
