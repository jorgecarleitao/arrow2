use crate::array::PrimitiveArray;
use crate::compute::comparison::primitive_compare_values_op;
use crate::datatypes::DataType;
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

/// Returns whether [`nulliff`] is implemented for the datatypes.
pub fn can_nullif(lhs: &DataType, rhs: &DataType) -> bool {
    if lhs != rhs {
        return false;
    };
    use DataType::*;
    matches!(
        lhs,
        UInt8
            | UInt16
            | UInt32
            | UInt64
            | Int8
            | Int16
            | Int32
            | Int64
            | Float32
            | Float64
            | Time32(_)
            | Time64(_)
            | Date32
            | Date64
            | Timestamp(_, _)
            | Duration(_)
    )
}

/// Returns an array whose validity is null iff `lhs == rhs` or `lhs` is null.
/// This has the same semantics as postgres.
/// # Example
/// ```rust
/// # use arrow2::array::Int32Array;
/// # use arrow2::datatypes::DataType;
/// # use arrow2::error::Result;
/// # use arrow2::compute::nullif::nullif;
/// # fn main() -> Result<()> {
/// let lhs = Int32Array::from(&[None, None, Some(1), Some(1), Some(1)]);
/// let rhs = Int32Array::from(&[None, Some(1), None, Some(1), Some(0)]);
/// let result = nullif(&lhs, &rhs)?;
///
/// let expected = Int32Array::from(&[None, None, Some(1), None, Some(1)]);
///
/// assert_eq!(expected, result.as_ref());
/// Ok(())
/// # }
/// ```
/// # Errors
/// This function errors iff
/// * The arguments do not have the same logical type
/// * The arguments do not have the same length
/// * The logical type is not supported
pub fn nullif(lhs: &dyn Array, rhs: &dyn Array) -> Result<Box<dyn Array>> {
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Nullif expects arrays of the the same logical type".to_string(),
        ));
    }
    if lhs.len() != rhs.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Nullif expects arrays of the the same length".to_string(),
        ));
    }
    use crate::datatypes::DataType::*;
    match lhs.data_type() {
        UInt8 => nullif_primitive::<u8>(
            lhs.as_any().downcast_ref().unwrap(),
            rhs.as_any().downcast_ref().unwrap(),
        )
        .map(|x| Box::new(x) as Box<dyn Array>),
        UInt16 => nullif_primitive::<u16>(
            lhs.as_any().downcast_ref().unwrap(),
            rhs.as_any().downcast_ref().unwrap(),
        )
        .map(|x| Box::new(x) as Box<dyn Array>),
        UInt32 => nullif_primitive::<u32>(
            lhs.as_any().downcast_ref().unwrap(),
            rhs.as_any().downcast_ref().unwrap(),
        )
        .map(|x| Box::new(x) as Box<dyn Array>),
        UInt64 => nullif_primitive::<u64>(
            lhs.as_any().downcast_ref().unwrap(),
            rhs.as_any().downcast_ref().unwrap(),
        )
        .map(|x| Box::new(x) as Box<dyn Array>),
        Int8 => nullif_primitive::<i8>(
            lhs.as_any().downcast_ref().unwrap(),
            rhs.as_any().downcast_ref().unwrap(),
        )
        .map(|x| Box::new(x) as Box<dyn Array>),
        Int16 => nullif_primitive::<i16>(
            lhs.as_any().downcast_ref().unwrap(),
            rhs.as_any().downcast_ref().unwrap(),
        )
        .map(|x| Box::new(x) as Box<dyn Array>),
        Int32 | Time32(_) | Date32 => nullif_primitive::<i32>(
            lhs.as_any().downcast_ref().unwrap(),
            rhs.as_any().downcast_ref().unwrap(),
        )
        .map(|x| Box::new(x) as Box<dyn Array>),
        Int64 | Time64(_) | Date64 | Timestamp(_, _) | Duration(_) => nullif_primitive::<i64>(
            lhs.as_any().downcast_ref().unwrap(),
            rhs.as_any().downcast_ref().unwrap(),
        )
        .map(|x| Box::new(x) as Box<dyn Array>),
        Float32 => nullif_primitive::<f32>(
            lhs.as_any().downcast_ref().unwrap(),
            rhs.as_any().downcast_ref().unwrap(),
        )
        .map(|x| Box::new(x) as Box<dyn Array>),
        Float64 => nullif_primitive::<f64>(
            lhs.as_any().downcast_ref().unwrap(),
            rhs.as_any().downcast_ref().unwrap(),
        )
        .map(|x| Box::new(x) as Box<dyn Array>),
        other => Err(ArrowError::NotYetImplemented(format!(
            "Nullif is not implemented for logical datatype {}",
            other
        ))),
    }
}
