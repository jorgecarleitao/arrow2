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

use crate::array::*;
use crate::bitmap::Bitmap;
use crate::{
    bitmap::MutableBitmap,
    error::{ArrowError, Result},
};

use super::{super::utils::combine_validities, Operator};

pub fn compare_values_op<F>(lhs: &Bitmap, rhs: &Bitmap, op: F) -> MutableBitmap
where
    F: Fn(bool, bool) -> bool,
{
    assert_eq!(lhs.len(), rhs.len());
    let lhs_iter = lhs.iter();
    let rhs_iter = rhs.iter();

    MutableBitmap::from_trusted_len_iter(lhs_iter.zip(rhs_iter).map(|(x, y)| op(x, y)))
}

/// Evaluate `op(lhs, rhs)` for [`BooleanArray`]s using a specified
/// comparison function.
fn compare_op<F>(lhs: &BooleanArray, rhs: &BooleanArray, op: F) -> Result<BooleanArray>
where
    F: Fn(bool, bool) -> bool,
{
    if lhs.len() != rhs.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Cannot perform comparison operation on arrays of different length".to_string(),
        ));
    }

    let validity = combine_validities(lhs.validity(), rhs.validity());

    let values = compare_values_op(lhs.values(), rhs.values(), op);

    Ok(BooleanArray::from_data(values.into(), validity))
}

/// Evaluate `op(left, right)` for [`BooleanArray`] and scalar using
/// a specified comparison function.
pub fn compare_op_scalar<F>(lhs: &BooleanArray, rhs: bool, op: F) -> Result<BooleanArray>
where
    F: Fn(bool, bool) -> bool,
{
    let lhs_iter = lhs.values().iter();

    let values = Bitmap::from_trusted_len_iter(lhs_iter.map(|x| op(x, rhs)));
    Ok(BooleanArray::from_data(values, lhs.validity().clone()))
}

/// Perform `lhs == rhs` operation on two arrays.
pub fn eq(lhs: &BooleanArray, rhs: &BooleanArray) -> Result<BooleanArray> {
    compare_op(lhs, rhs, |a, b| a == b)
}

/// Perform `left == right` operation on an array and a scalar value.
pub fn eq_scalar(lhs: &BooleanArray, rhs: bool) -> Result<BooleanArray> {
    compare_op_scalar(lhs, rhs, |a, b| a == b)
}

/// Perform `left != right` operation on two arrays.
pub fn neq(lhs: &BooleanArray, rhs: &BooleanArray) -> Result<BooleanArray> {
    compare_op(lhs, rhs, |a, b| a != b)
}

/// Perform `left != right` operation on an array and a scalar value.
pub fn neq_scalar(lhs: &BooleanArray, rhs: bool) -> Result<BooleanArray> {
    compare_op_scalar(lhs, rhs, |a, b| a != b)
}

/// Perform `left < right` operation on two arrays.
pub fn lt(lhs: &BooleanArray, rhs: &BooleanArray) -> Result<BooleanArray> {
    compare_op(lhs, rhs, |a, b| !a & b)
}

/// Perform `left < right` operation on an array and a scalar value.
pub fn lt_scalar(lhs: &BooleanArray, rhs: bool) -> Result<BooleanArray> {
    compare_op_scalar(lhs, rhs, |a, b| !a & b)
}

/// Perform `left <= right` operation on two arrays.
pub fn lt_eq(lhs: &BooleanArray, rhs: &BooleanArray) -> Result<BooleanArray> {
    compare_op(lhs, rhs, |a, b| a <= b)
}

/// Perform `left <= right` operation on an array and a scalar value.
/// Null values are less than non-null values.
pub fn lt_eq_scalar(lhs: &BooleanArray, rhs: bool) -> Result<BooleanArray> {
    compare_op_scalar(lhs, rhs, |a, b| a <= b)
}

/// Perform `left > right` operation on two arrays. Non-null values are greater than null
/// values.
pub fn gt(lhs: &BooleanArray, rhs: &BooleanArray) -> Result<BooleanArray> {
    compare_op(lhs, rhs, |a, b| a & !b)
}

/// Perform `left > right` operation on an array and a scalar value.
/// Non-null values are greater than null values.
pub fn gt_scalar(lhs: &BooleanArray, rhs: bool) -> Result<BooleanArray> {
    compare_op_scalar(lhs, rhs, |a, b| a & !b)
}

/// Perform `left >= right` operation on two arrays. Non-null values are greater than null
/// values.
pub fn gt_eq(lhs: &BooleanArray, rhs: &BooleanArray) -> Result<BooleanArray> {
    compare_op(lhs, rhs, |a, b| a >= b)
}

/// Perform `left >= right` operation on an array and a scalar value.
/// Non-null values are greater than null values.
pub fn gt_eq_scalar(lhs: &BooleanArray, rhs: bool) -> Result<BooleanArray> {
    compare_op_scalar(lhs, rhs, |a, b| a >= b)
}

pub fn compare(lhs: &BooleanArray, rhs: &BooleanArray, op: Operator) -> Result<BooleanArray> {
    match op {
        Operator::Eq => eq(lhs, rhs),
        Operator::Neq => neq(lhs, rhs),
        Operator::Gt => gt(lhs, rhs),
        Operator::GtEq => gt_eq(lhs, rhs),
        Operator::Lt => lt(lhs, rhs),
        Operator::LtEq => lt_eq(lhs, rhs),
    }
}

pub fn compare_scalar(lhs: &BooleanArray, rhs: bool, op: Operator) -> Result<BooleanArray> {
    match op {
        Operator::Eq => eq_scalar(lhs, rhs),
        Operator::Neq => neq_scalar(lhs, rhs),
        Operator::Gt => gt_scalar(lhs, rhs),
        Operator::GtEq => gt_eq_scalar(lhs, rhs),
        Operator::Lt => lt_scalar(lhs, rhs),
        Operator::LtEq => lt_eq_scalar(lhs, rhs),
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
            let c = $KERNEL(&a, &b).unwrap();
            assert_eq!(BooleanArray::from_slice($EXPECTED), c);
        };
    }

    macro_rules! cmp_bool_options {
        ($KERNEL:ident, $A_VEC:expr, $B_VEC:expr, $EXPECTED:expr) => {
            let a = BooleanArray::from($A_VEC);
            let b = BooleanArray::from($B_VEC);
            let c = $KERNEL(&a, &b).unwrap();
            assert_eq!(BooleanArray::from($EXPECTED), c);
        };
    }

    macro_rules! cmp_bool_scalar {
        ($KERNEL:ident, $A_VEC:expr, $B:literal, $EXPECTED:expr) => {
            let a = BooleanArray::from_slice($A_VEC);
            let c = $KERNEL(&a, $B).unwrap();
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
        let d = eq(&c, &a).unwrap();
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
    fn test_lt_scalar() {
        cmp_bool_scalar!(lt_scalar, &[false, true], true, &[true, false]);
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
