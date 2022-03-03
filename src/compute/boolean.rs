//! null-preserving operators such as [`and`], [`or`] and [`not`].
use crate::array::{Array, BooleanArray};
use crate::bitmap::{Bitmap, MutableBitmap};
use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};
use crate::scalar::BooleanScalar;

use super::utils::combine_validities;

/// Helper function to implement binary kernels
fn binary_boolean_kernel<F>(lhs: &BooleanArray, rhs: &BooleanArray, op: F) -> Result<BooleanArray>
where
    F: Fn(&Bitmap, &Bitmap) -> Bitmap,
{
    if lhs.len() != rhs.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Cannot perform bitwise operation on arrays of different length".to_string(),
        ));
    }

    let validity = combine_validities(lhs.validity(), rhs.validity());

    let left_buffer = lhs.values();
    let right_buffer = rhs.values();

    let values = op(left_buffer, right_buffer);

    Ok(BooleanArray::new(DataType::Boolean, values, validity))
}

/// Performs `AND` operation on two arrays. If either left or right value is null then the
/// result is also null.
/// # Error
/// This function errors when the arrays have different lengths.
/// # Example
/// ```rust
/// use arrow2::array::BooleanArray;
/// use arrow2::error::Result;
/// use arrow2::compute::boolean::and;
/// # fn main() -> Result<()> {
/// let a = BooleanArray::from(&[Some(false), Some(true), None]);
/// let b = BooleanArray::from(&[Some(true), Some(true), Some(false)]);
/// let and_ab = and(&a, &b)?;
/// assert_eq!(and_ab, BooleanArray::from(&[Some(false), Some(true), None]));
/// # Ok(())
/// # }
/// ```
pub fn and(lhs: &BooleanArray, rhs: &BooleanArray) -> Result<BooleanArray> {
    binary_boolean_kernel(lhs, rhs, |lhs, rhs| lhs & rhs)
}

/// Performs `OR` operation on two arrays. If either left or right value is null then the
/// result is also null.
/// # Error
/// This function errors when the arrays have different lengths.
/// # Example
/// ```rust
/// use arrow2::array::BooleanArray;
/// use arrow2::error::Result;
/// use arrow2::compute::boolean::or;
/// # fn main() -> Result<()> {
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let b = BooleanArray::from(vec![Some(true), Some(true), Some(false)]);
/// let or_ab = or(&a, &b)?;
/// assert_eq!(or_ab, BooleanArray::from(vec![Some(true), Some(true), None]));
/// # Ok(())
/// # }
/// ```
pub fn or(lhs: &BooleanArray, rhs: &BooleanArray) -> Result<BooleanArray> {
    binary_boolean_kernel(lhs, rhs, |lhs, rhs| lhs | rhs)
}

/// Performs unary `NOT` operation on an arrays. If value is null then the result is also
/// null.
/// # Example
/// ```rust
/// use arrow2::array::BooleanArray;
/// use arrow2::compute::boolean::not;
/// # fn main() {
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let not_a = not(&a);
/// assert_eq!(not_a, BooleanArray::from(vec![Some(true), Some(false), None]));
/// # }
/// ```
pub fn not(array: &BooleanArray) -> BooleanArray {
    let values = !array.values();
    let validity = array.validity().cloned();
    BooleanArray::new(DataType::Boolean, values, validity)
}

/// Returns a non-null [BooleanArray] with whether each value of the array is null.
/// # Error
/// This function never errors.
/// # Example
/// ```rust
/// use arrow2::array::BooleanArray;
/// use arrow2::compute::boolean::is_null;
/// # fn main() {
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let a_is_null = is_null(&a);
/// assert_eq!(a_is_null, BooleanArray::from_slice(vec![false, false, true]));
/// # }
/// ```
pub fn is_null(input: &dyn Array) -> BooleanArray {
    let len = input.len();

    let values = match input.validity() {
        None => MutableBitmap::from_len_zeroed(len).into(),
        Some(buffer) => !buffer,
    };

    BooleanArray::new(DataType::Boolean, values, None)
}

/// Returns a non-null [BooleanArray] with whether each value of the array is not null.
/// # Example
/// ```rust
/// use arrow2::array::BooleanArray;
/// use arrow2::compute::boolean::is_not_null;
/// # fn main() {
/// let a = BooleanArray::from(&vec![Some(false), Some(true), None]);
/// let a_is_not_null = is_not_null(&a);
/// assert_eq!(a_is_not_null, BooleanArray::from_slice(&vec![true, true, false]));
/// # }
/// ```
pub fn is_not_null(input: &dyn Array) -> BooleanArray {
    let values = match input.validity() {
        None => {
            let mut mutable = MutableBitmap::new();
            mutable.extend_constant(input.len(), true);
            mutable.into()
        }
        Some(buffer) => buffer.clone(),
    };
    BooleanArray::new(DataType::Boolean, values, None)
}

/// Performs `AND` operation on an array and a scalar value. If either left or right value
/// is null then the result is also null.
/// # Example
/// ```rust
/// use arrow2::array::BooleanArray;
/// use arrow2::compute::boolean::and_scalar;
/// use arrow2::scalar::BooleanScalar;
/// # fn main() {
/// let array = BooleanArray::from_slice(&[false, false, true, true]);
/// let scalar = BooleanScalar::new(Some(true));
/// let result = and_scalar(&array, &scalar);
/// assert_eq!(result, BooleanArray::from_slice(&[false, false, true, true]));
/// # }
/// ```
pub fn and_scalar(array: &BooleanArray, scalar: &BooleanScalar) -> BooleanArray {
    match scalar.value() {
        Some(true) => array.clone(),
        Some(false) => {
            let values = Bitmap::new_zeroed(array.len());
            BooleanArray::new(DataType::Boolean, values, array.validity().cloned())
        }
        None => BooleanArray::new_null(DataType::Boolean, array.len()),
    }
}

/// Performs `OR` operation on an array and a scalar value. If either left or right value
/// is null then the result is also null.
/// # Example
/// ```rust
/// use arrow2::array::BooleanArray;
/// use arrow2::compute::boolean::or_scalar;
/// use arrow2::scalar::BooleanScalar;
/// # fn main() {
/// let array = BooleanArray::from_slice(&[false, false, true, true]);
/// let scalar = BooleanScalar::new(Some(true));
/// let result = or_scalar(&array, &scalar);
/// assert_eq!(result, BooleanArray::from_slice(&[true, true, true, true]));
/// # }
/// ```
pub fn or_scalar(array: &BooleanArray, scalar: &BooleanScalar) -> BooleanArray {
    match scalar.value() {
        Some(true) => {
            let mut values = MutableBitmap::new();
            values.extend_constant(array.len(), true);
            BooleanArray::new(DataType::Boolean, values.into(), array.validity().cloned())
        }
        Some(false) => array.clone(),
        None => BooleanArray::new_null(DataType::Boolean, array.len()),
    }
}

/// Check if any of the values in the array is `true`
pub fn any(array: &BooleanArray) -> bool {
    if array.is_empty() {
        false
    } else if array.validity().is_some() {
        array.into_iter().any(|v| v == Some(true))
    } else {
        let vals = array.values();
        vals.null_count() != vals.len()
    }
}

/// Check if all of the values in the array are `true`
pub fn all(array: &BooleanArray) -> bool {
    if array.is_empty() || array.null_count() > 0 {
        false
    } else {
        let vals = array.values();
        vals.null_count() == 0
    }
}
