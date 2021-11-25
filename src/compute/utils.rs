use crate::{
    array::{Array, BooleanArray, Offset, Utf8Array},
    bitmap::Bitmap,
    datatypes::DataType,
    error::{ArrowError, Result},
};

pub fn combine_validities(lhs: Option<&Bitmap>, rhs: Option<&Bitmap>) -> Option<Bitmap> {
    match (lhs, rhs) {
        (Some(lhs), None) => Some(lhs.clone()),
        (None, Some(rhs)) => Some(rhs.clone()),
        (None, None) => None,
        (Some(lhs), Some(rhs)) => Some(lhs & rhs),
    }
}

pub fn unary_utf8_boolean<O: Offset, F: Fn(&str) -> bool>(
    values: &Utf8Array<O>,
    op: F,
) -> BooleanArray {
    let validity = values.validity().cloned();

    let iterator = values.iter().map(|value| {
        if value.is_none() {
            return false;
        };
        op(value.unwrap())
    });
    let values = Bitmap::from_trusted_len_iter(iterator);
    BooleanArray::from_data(DataType::Boolean, values, validity)
}

// Errors iff the two arrays have a different length.
#[inline]
pub fn check_same_len(lhs: &dyn Array, rhs: &dyn Array) -> Result<()> {
    if lhs.len() != rhs.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same length".to_string(),
        ));
    }
    Ok(())
}

// Errors iff the two arrays have a different data_type.
#[inline]
pub fn check_same_type(lhs: &dyn Array, rhs: &dyn Array) -> Result<()> {
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same logical type".to_string(),
        ));
    }
    Ok(())
}
