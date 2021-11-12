//! null-preserving operators such as [`and`], [`or`] and [`not`].
use crate::array::{Array, BooleanArray};
use crate::bitmap::{Bitmap, MutableBitmap};
use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};

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

    Ok(BooleanArray::from_data(DataType::Boolean, values, validity))
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
    BooleanArray::from_data(DataType::Boolean, values, validity)
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

    BooleanArray::from_data(DataType::Boolean, values, None)
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
    BooleanArray::from_data(DataType::Boolean, values, None)
}
