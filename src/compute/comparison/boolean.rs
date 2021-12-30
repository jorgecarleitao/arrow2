//! Comparison functions for [`BooleanArray`]
use crate::{
    array::BooleanArray,
    bitmap::{binary, unary, Bitmap},
    datatypes::DataType,
};

use super::super::utils::combine_validities;

/// Evaluate `op(lhs, rhs)` for [`BooleanArray`]s using a specified
/// comparison function.
fn compare_op<F>(lhs: &BooleanArray, rhs: &BooleanArray, op: F) -> BooleanArray
where
    F: Fn(u64, u64) -> u64,
{
    assert_eq!(lhs.len(), rhs.len());
    let validity = combine_validities(lhs.validity(), rhs.validity());

    let values = binary(lhs.values(), rhs.values(), op);

    BooleanArray::from_data(DataType::Boolean, values, validity)
}

/// Evaluate `op(left, right)` for [`BooleanArray`] and scalar using
/// a specified comparison function.
pub fn compare_op_scalar<F>(lhs: &BooleanArray, rhs: bool, op: F) -> BooleanArray
where
    F: Fn(u64, u64) -> u64,
{
    let rhs = if rhs { !0 } else { 0 };

    let values = unary(lhs.values(), |x| op(x, rhs));
    BooleanArray::from_data(DataType::Boolean, values, lhs.validity().cloned())
}

/// Perform `lhs == rhs` operation on two [`BooleanArray`]s.
pub fn eq(lhs: &BooleanArray, rhs: &BooleanArray) -> BooleanArray {
    compare_op(lhs, rhs, |a, b| !(a ^ b))
}

/// Perform `lhs == rhs` operation on a [`BooleanArray`] and a scalar value.
pub fn eq_scalar(lhs: &BooleanArray, rhs: bool) -> BooleanArray {
    if rhs {
        lhs.clone()
    } else {
        compare_op_scalar(lhs, rhs, |a, _| !a)
    }
}

/// `lhs != rhs` for [`BooleanArray`]
pub fn neq(lhs: &BooleanArray, rhs: &BooleanArray) -> BooleanArray {
    compare_op(lhs, rhs, |a, b| a ^ b)
}

/// Perform `left != right` operation on an array and a scalar value.
pub fn neq_scalar(lhs: &BooleanArray, rhs: bool) -> BooleanArray {
    eq_scalar(lhs, !rhs)
}

/// Perform `left < right` operation on two arrays.
pub fn lt(lhs: &BooleanArray, rhs: &BooleanArray) -> BooleanArray {
    compare_op(lhs, rhs, |a, b| !a & b)
}

/// Perform `left < right` operation on an array and a scalar value.
pub fn lt_scalar(lhs: &BooleanArray, rhs: bool) -> BooleanArray {
    if rhs {
        compare_op_scalar(lhs, rhs, |a, _| !a)
    } else {
        BooleanArray::from_data(
            DataType::Boolean,
            Bitmap::new_zeroed(lhs.len()),
            lhs.validity().cloned(),
        )
    }
}

/// Perform `left <= right` operation on two arrays.
pub fn lt_eq(lhs: &BooleanArray, rhs: &BooleanArray) -> BooleanArray {
    compare_op(lhs, rhs, |a, b| !a | b)
}

/// Perform `left <= right` operation on an array and a scalar value.
/// Null values are less than non-null values.
pub fn lt_eq_scalar(lhs: &BooleanArray, rhs: bool) -> BooleanArray {
    if rhs {
        let all_ones = !0;
        compare_op_scalar(lhs, rhs, |_, _| all_ones)
    } else {
        compare_op_scalar(lhs, rhs, |a, _| !a)
    }
}

/// Perform `left > right` operation on two arrays. Non-null values are greater than null
/// values.
pub fn gt(lhs: &BooleanArray, rhs: &BooleanArray) -> BooleanArray {
    compare_op(lhs, rhs, |a, b| a & !b)
}

/// Perform `left > right` operation on an array and a scalar value.
/// Non-null values are greater than null values.
pub fn gt_scalar(lhs: &BooleanArray, rhs: bool) -> BooleanArray {
    if rhs {
        BooleanArray::from_data(
            DataType::Boolean,
            Bitmap::new_zeroed(lhs.len()),
            lhs.validity().cloned(),
        )
    } else {
        lhs.clone()
    }
}

/// Perform `left >= right` operation on two arrays. Non-null values are greater than null
/// values.
pub fn gt_eq(lhs: &BooleanArray, rhs: &BooleanArray) -> BooleanArray {
    compare_op(lhs, rhs, |a, b| a | !b)
}

/// Perform `left >= right` operation on an array and a scalar value.
/// Non-null values are greater than null values.
pub fn gt_eq_scalar(lhs: &BooleanArray, rhs: bool) -> BooleanArray {
    if rhs {
        lhs.clone()
    } else {
        let all_ones = !0;
        compare_op_scalar(lhs, rhs, |_, _| all_ones)
    }
}
