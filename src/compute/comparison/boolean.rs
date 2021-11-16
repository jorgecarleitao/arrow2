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

    BooleanArray::from_data(DataType::Boolean, values.into(), validity)
}

/// Evaluate `op(left, right)` for [`BooleanArray`] and scalar using
/// a specified comparison function.
pub fn compare_op_scalar<F>(lhs: &BooleanArray, rhs: bool, op: F) -> BooleanArray
where
    F: Fn(u64, u64) -> u64,
{
    let rhs = if rhs { 0b11111111 } else { 0 };

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
        compare_op_scalar(lhs, rhs, |_, _| 0b11111111)
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
        compare_op_scalar(lhs, rhs, |_, _| 0b11111111)
    }
}

// disable wrapping inside literal vectors used for test data and assertions
#[rustfmt::skip::macros(vec)]
#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! cmp_bool {
        ($KERNEL:ident, $A_VEC:expr, $B_VEC:expr, $EXPECTED:expr) => {
            let a = BooleanArray::from_slice($A_VEC);
            let b = BooleanArray::from_slice($B_VEC);
            let c = $KERNEL(&a, &b);
            assert_eq!(BooleanArray::from_slice($EXPECTED), c);
        };
    }

    macro_rules! cmp_bool_options {
        ($KERNEL:ident, $A_VEC:expr, $B_VEC:expr, $EXPECTED:expr) => {
            let a = BooleanArray::from($A_VEC);
            let b = BooleanArray::from($B_VEC);
            let c = $KERNEL(&a, &b);
            assert_eq!(BooleanArray::from($EXPECTED), c);
        };
    }

    macro_rules! cmp_bool_scalar {
        ($KERNEL:ident, $A_VEC:expr, $B:literal, $EXPECTED:expr) => {
            let a = BooleanArray::from_slice($A_VEC);
            let c = $KERNEL(&a, $B);
            assert_eq!(BooleanArray::from_slice($EXPECTED), c);
        };
    }

    #[test]
    fn test_eq() {
        cmp_bool!(
            eq,
            &[true, false, true, false],
            &[true, true, false, false],
            &[true, false, false, true]
        );
    }

    #[test]
    fn test_eq_scalar() {
        cmp_bool_scalar!(eq_scalar, &[false, true], true, &[false, true]);
    }

    #[test]
    fn test_eq_with_slice() {
        let a = BooleanArray::from_slice(&[true, true, false]);
        let b = BooleanArray::from_slice(&[true, true, true, true, false]);
        let c = b.slice(2, 3);
        let d = eq(&c, &a);
        assert_eq!(d, BooleanArray::from_slice(&[true, true, true]));
    }

    #[test]
    fn test_neq() {
        cmp_bool!(
            neq,
            &[true, false, true, false],
            &[true, true, false, false],
            &[false, true, true, false]
        );
    }

    #[test]
    fn test_neq_scalar() {
        cmp_bool_scalar!(neq_scalar, &[false, true], true, &[true, false]);
    }

    #[test]
    fn test_lt() {
        cmp_bool!(
            lt,
            &[true, false, true, false],
            &[true, true, false, false],
            &[false, true, false, false]
        );
    }

    #[test]
    fn test_lt_scalar_true() {
        cmp_bool_scalar!(lt_scalar, &[false, true], true, &[true, false]);
    }

    #[test]
    fn test_lt_scalar_false() {
        cmp_bool_scalar!(lt_scalar, &[false, true], false, &[false, false]);
    }

    #[test]
    fn test_lt_eq_scalar_true() {
        cmp_bool_scalar!(lt_eq_scalar, &[false, true], true, &[true, true]);
    }

    #[test]
    fn test_lt_eq_scalar_false() {
        cmp_bool_scalar!(lt_eq_scalar, &[false, true], false, &[true, false]);
    }

    #[test]
    fn test_gt_scalar_true() {
        cmp_bool_scalar!(gt_scalar, &[false, true], true, &[false, false]);
    }

    #[test]
    fn test_gt_scalar_false() {
        cmp_bool_scalar!(gt_scalar, &[false, true], false, &[false, true]);
    }

    #[test]
    fn test_gt_eq_scalar_true() {
        cmp_bool_scalar!(gt_eq_scalar, &[false, true], true, &[false, true]);
    }

    #[test]
    fn test_gt_eq_scalar_false() {
        cmp_bool_scalar!(gt_eq_scalar, &[false, true], false, &[true, true]);
    }

    #[test]
    fn eq_nulls() {
        cmp_bool_options!(
            eq,
            &[
                None,
                None,
                None,
                Some(false),
                Some(false),
                Some(false),
                Some(true),
                Some(true),
                Some(true)
            ],
            &[
                None,
                Some(false),
                Some(true),
                None,
                Some(false),
                Some(true),
                None,
                Some(false),
                Some(true)
            ],
            &[
                None,
                None,
                None,
                None,
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(true)
            ]
        );
    }
}
