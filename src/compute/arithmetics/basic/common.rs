use crate::array::{Array, PrimitiveArray};
use crate::error::{ArrowError, Result};
use crate::types::NativeType;

// Checking if both arrays have the same type
#[inline]
pub fn check_same_type<L: NativeType, R: NativeType>(
    lhs: &PrimitiveArray<L>,
    rhs: &PrimitiveArray<R>,
) -> Result<()> {
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }
    Ok(())
}

// Checking if both arrays have the same length
#[inline]
pub fn check_same_len<L: NativeType, R: NativeType>(
    lhs: &PrimitiveArray<L>,
    rhs: &PrimitiveArray<R>,
) -> Result<()> {
    if lhs.len() != rhs.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same length".to_string(),
        ));
    }
    Ok(())
}
