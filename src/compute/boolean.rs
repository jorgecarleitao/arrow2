// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::array::{Array, BooleanArray};
use crate::bitmap::{Bitmap, MutableBitmap};
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

    let values = op(&left_buffer, &right_buffer);

    Ok(BooleanArray::from_data(values, validity))
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
    binary_boolean_kernel(&lhs, &rhs, |lhs, rhs| lhs & rhs)
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
    binary_boolean_kernel(&lhs, &rhs, |lhs, rhs| lhs | rhs)
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
    let validity = array.validity().clone();
    BooleanArray::from_data(values, validity)
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

    BooleanArray::from_data(values, None)
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
        None => Bitmap::from_trusted_len_iter(std::iter::repeat(true).take(input.len())),
        Some(buffer) => buffer.clone(),
    };
    BooleanArray::from_data(values, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::*;

    #[test]
    fn test_bool_array_and() {
        let a = BooleanArray::from_slice(vec![false, false, true, true]);
        let b = BooleanArray::from_slice(vec![false, true, false, true]);
        let c = and(&a, &b).unwrap();

        let expected = BooleanArray::from_slice(vec![false, false, false, true]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_or() {
        let a = BooleanArray::from_slice(vec![false, false, true, true]);
        let b = BooleanArray::from_slice(vec![false, true, false, true]);
        let c = or(&a, &b).unwrap();

        let expected = BooleanArray::from_slice(vec![false, true, true, true]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_or_validity() {
        let a = BooleanArray::from(vec![
            None,
            None,
            None,
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
        ]);
        let b = BooleanArray::from(vec![
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
        ]);
        let c = or(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            None,
            None,
            None,
            None,
            Some(false),
            Some(true),
            None,
            Some(true),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_not() {
        let a = BooleanArray::from_slice(vec![false, true]);
        let c = not(&a);

        let expected = BooleanArray::from_slice(vec![true, false]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_and_validity() {
        let a = BooleanArray::from(vec![
            None,
            None,
            None,
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
        ]);
        let b = BooleanArray::from(vec![
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
        ]);
        let c = and(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            None,
            None,
            None,
            None,
            Some(false),
            Some(false),
            None,
            Some(false),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_and_sliced_same_offset() {
        let a = BooleanArray::from_slice(vec![
            false, false, false, false, false, false, false, false, false, false, true, true,
        ]);
        let b = BooleanArray::from_slice(vec![
            false, false, false, false, false, false, false, false, false, true, false, true,
        ]);

        let a = a.slice(8, 4);
        let b = b.slice(8, 4);
        let c = and(&a, &b).unwrap();

        let expected = BooleanArray::from_slice(vec![false, false, false, true]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_bool_array_and_sliced_same_offset_mod8() {
        let a = BooleanArray::from_slice(vec![
            false, false, true, true, false, false, false, false, false, false, false, false,
        ]);
        let b = BooleanArray::from_slice(vec![
            false, false, false, false, false, false, false, false, false, true, false, true,
        ]);

        let a = a.slice(0, 4);
        let b = b.slice(8, 4);

        let c = and(&a, &b).unwrap();

        let expected = BooleanArray::from_slice(vec![false, false, false, true]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_bool_array_and_sliced_offset1() {
        let a = BooleanArray::from_slice(vec![
            false, false, false, false, false, false, false, false, false, false, true, true,
        ]);
        let b = BooleanArray::from_slice(vec![false, true, false, true]);

        let a = a.slice(8, 4);

        let c = and(&a, &b).unwrap();

        let expected = BooleanArray::from_slice(vec![false, false, false, true]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_bool_array_and_sliced_offset2() {
        let a = BooleanArray::from_slice(vec![false, false, true, true]);
        let b = BooleanArray::from_slice(vec![
            false, false, false, false, false, false, false, false, false, true, false, true,
        ]);

        let b = b.slice(8, 4);

        let c = and(&a, &b).unwrap();

        let expected = BooleanArray::from_slice(vec![false, false, false, true]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_bool_array_and_validity_offset() {
        let a = BooleanArray::from(vec![None, Some(false), Some(true), None, Some(true)]);
        let a = a.slice(1, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();

        let b = BooleanArray::from(vec![
            None,
            None,
            Some(true),
            Some(false),
            Some(true),
            Some(true),
        ]);

        let b = b.slice(2, 4);
        let b = b.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![Some(false), Some(false), None, Some(true)]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_nonnull_array_is_null() {
        let a = Int32Array::from_slice(&[1, 2, 3, 4]);

        let res = is_null(&a);

        let expected = BooleanArray::from_slice(vec![false, false, false, false]);

        assert_eq!(expected, res);
    }

    #[test]
    fn test_nonnull_array_with_offset_is_null() {
        let a = Int32Array::from_slice(vec![1, 2, 3, 4, 5, 6, 7, 8, 7, 6, 5, 4, 3, 2, 1]);
        let a = a.slice(8, 4);

        let res = is_null(&a);

        let expected = BooleanArray::from_slice(vec![false, false, false, false]);

        assert_eq!(expected, res);
    }

    #[test]
    fn test_nonnull_array_is_not_null() {
        let a = Int32Array::from_slice(&[1, 2, 3, 4]);

        let res = is_not_null(&a);

        let expected = BooleanArray::from_slice(vec![true, true, true, true]);

        assert_eq!(expected, res);
    }

    #[test]
    fn test_nonnull_array_with_offset_is_not_null() {
        let a = Int32Array::from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 7, 6, 5, 4, 3, 2, 1]);
        let a = a.slice(8, 4);

        let res = is_not_null(&a);

        let expected = BooleanArray::from_slice(&[true, true, true, true]);

        assert_eq!(expected, res);
    }

    #[test]
    fn test_nullable_array_is_null() {
        let a = Int32Array::from(vec![Some(1), None, Some(3), None]);

        let res = is_null(&a);

        let expected = BooleanArray::from_slice(vec![false, true, false, true]);

        assert_eq!(expected, res);
    }

    #[test]
    fn test_nullable_array_with_offset_is_null() {
        let a = Int32Array::from(vec![
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            // offset 8, previous None values are skipped by the slice
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
            Some(4),
            None,
            None,
        ]);
        let a = a.slice(8, 4);

        let res = is_null(&a);

        let expected = BooleanArray::from_slice(vec![false, true, false, true]);

        assert_eq!(expected, res);
    }

    #[test]
    fn test_nullable_array_is_not_null() {
        let a = Int32Array::from(vec![Some(1), None, Some(3), None]);

        let res = is_not_null(&a);

        let expected = BooleanArray::from_slice(vec![true, false, true, false]);

        assert_eq!(expected, res);
    }

    #[test]
    fn test_nullable_array_with_offset_is_not_null() {
        let a = Int32Array::from(vec![
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            // offset 8, previous None values are skipped by the slice
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
            Some(4),
            None,
            None,
        ]);
        let a = a.slice(8, 4);

        let res = is_not_null(&a);

        let expected = BooleanArray::from_slice(vec![true, false, true, false]);

        assert_eq!(expected, res);
    }
}
