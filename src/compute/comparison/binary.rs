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

use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};
use crate::scalar::{BinaryScalar, Scalar};
use crate::{array::*, bitmap::Bitmap};

use super::{super::utils::combine_validities, Operator};

/// Evaluate `op(lhs, rhs)` for [`BinaryArray`]s using a specified
/// comparison function.
fn compare_op<O, F>(lhs: &BinaryArray<O>, rhs: &BinaryArray<O>, op: F) -> Result<BooleanArray>
where
    O: Offset,
    F: Fn(&[u8], &[u8]) -> bool,
{
    if lhs.len() != rhs.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Cannot perform comparison operation on arrays of different length".to_string(),
        ));
    }

    let validity = combine_validities(lhs.validity(), rhs.validity());

    let values = lhs
        .values_iter()
        .zip(rhs.values_iter())
        .map(|(lhs, rhs)| op(lhs, rhs));
    let values = Bitmap::from_trusted_len_iter(values);

    Ok(BooleanArray::from_data(DataType::Boolean, values, validity))
}

/// Evaluate `op(lhs, rhs)` for [`BinaryArray`] and scalar using
/// a specified comparison function.
fn compare_op_scalar<O, F>(lhs: &BinaryArray<O>, rhs: &[u8], op: F) -> BooleanArray
where
    O: Offset,
    F: Fn(&[u8], &[u8]) -> bool,
{
    let validity = lhs.validity().clone();

    let values = lhs.values_iter().map(|lhs| op(lhs, rhs));
    let values = Bitmap::from_trusted_len_iter(values);

    BooleanArray::from_data(DataType::Boolean, values, validity)
}

/// Perform `lhs == rhs` operation on [`BinaryArray`].
fn eq<O: Offset>(lhs: &BinaryArray<O>, rhs: &BinaryArray<O>) -> Result<BooleanArray> {
    compare_op(lhs, rhs, |a, b| a == b)
}

/// Perform `lhs == rhs` operation on [`BinaryArray`] and a scalar.
fn eq_scalar<O: Offset>(lhs: &BinaryArray<O>, rhs: &[u8]) -> BooleanArray {
    compare_op_scalar(lhs, rhs, |a, b| a == b)
}

/// Perform `lhs != rhs` operation on [`BinaryArray`].
fn neq<O: Offset>(lhs: &BinaryArray<O>, rhs: &BinaryArray<O>) -> Result<BooleanArray> {
    compare_op(lhs, rhs, |a, b| a != b)
}

/// Perform `lhs != rhs` operation on [`BinaryArray`] and a scalar.
fn neq_scalar<O: Offset>(lhs: &BinaryArray<O>, rhs: &[u8]) -> BooleanArray {
    compare_op_scalar(lhs, rhs, |a, b| a != b)
}

/// Perform `lhs < rhs` operation on [`BinaryArray`].
fn lt<O: Offset>(lhs: &BinaryArray<O>, rhs: &BinaryArray<O>) -> Result<BooleanArray> {
    compare_op(lhs, rhs, |a, b| a < b)
}

/// Perform `lhs < rhs` operation on [`BinaryArray`] and a scalar.
fn lt_scalar<O: Offset>(lhs: &BinaryArray<O>, rhs: &[u8]) -> BooleanArray {
    compare_op_scalar(lhs, rhs, |a, b| a < b)
}

/// Perform `lhs <= rhs` operation on [`BinaryArray`].
fn lt_eq<O: Offset>(lhs: &BinaryArray<O>, rhs: &BinaryArray<O>) -> Result<BooleanArray> {
    compare_op(lhs, rhs, |a, b| a <= b)
}

/// Perform `lhs <= rhs` operation on [`BinaryArray`] and a scalar.
fn lt_eq_scalar<O: Offset>(lhs: &BinaryArray<O>, rhs: &[u8]) -> BooleanArray {
    compare_op_scalar(lhs, rhs, |a, b| a <= b)
}

/// Perform `lhs > rhs` operation on [`BinaryArray`].
fn gt<O: Offset>(lhs: &BinaryArray<O>, rhs: &BinaryArray<O>) -> Result<BooleanArray> {
    compare_op(lhs, rhs, |a, b| a > b)
}

/// Perform `lhs > rhs` operation on [`BinaryArray`] and a scalar.
fn gt_scalar<O: Offset>(lhs: &BinaryArray<O>, rhs: &[u8]) -> BooleanArray {
    compare_op_scalar(lhs, rhs, |a, b| a > b)
}

/// Perform `lhs >= rhs` operation on [`BinaryArray`].
fn gt_eq<O: Offset>(lhs: &BinaryArray<O>, rhs: &BinaryArray<O>) -> Result<BooleanArray> {
    compare_op(lhs, rhs, |a, b| a >= b)
}

/// Perform `lhs >= rhs` operation on [`BinaryArray`] and a scalar.
fn gt_eq_scalar<O: Offset>(lhs: &BinaryArray<O>, rhs: &[u8]) -> BooleanArray {
    compare_op_scalar(lhs, rhs, |a, b| a >= b)
}

/// Compare two [`BinaryArray`]s using the given [`Operator`].
///
/// # Errors
/// When the two arrays have different lengths.
///
/// Check the [crate::compute::comparison](module documentation) for usage
/// examples.
pub fn compare<O: Offset>(
    lhs: &BinaryArray<O>,
    rhs: &BinaryArray<O>,
    op: Operator,
) -> Result<BooleanArray> {
    match op {
        Operator::Eq => eq(lhs, rhs),
        Operator::Neq => neq(lhs, rhs),
        Operator::Gt => gt(lhs, rhs),
        Operator::GtEq => gt_eq(lhs, rhs),
        Operator::Lt => lt(lhs, rhs),
        Operator::LtEq => lt_eq(lhs, rhs),
    }
}

/// Compare a [`BinaryArray`] and a scalar value using the given
/// [`Operator`].
///
/// Check the [crate::compute::comparison](module documentation) for usage
/// examples.
pub fn compare_scalar<O: Offset>(
    lhs: &BinaryArray<O>,
    rhs: &BinaryScalar<O>,
    op: Operator,
) -> BooleanArray {
    if !rhs.is_valid() {
        return BooleanArray::new_null(DataType::Boolean, lhs.len());
    }
    compare_scalar_non_null(lhs, rhs.value(), op)
}

pub fn compare_scalar_non_null<O: Offset>(
    lhs: &BinaryArray<O>,
    rhs: &[u8],
    op: Operator,
) -> BooleanArray {
    match op {
        Operator::Eq => eq_scalar(lhs, rhs),
        Operator::Neq => neq_scalar(lhs, rhs),
        Operator::Gt => gt_scalar(lhs, rhs),
        Operator::GtEq => gt_eq_scalar(lhs, rhs),
        Operator::Lt => lt_scalar(lhs, rhs),
        Operator::LtEq => lt_eq_scalar(lhs, rhs),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_generic<O: Offset, F: Fn(&BinaryArray<O>, &BinaryArray<O>) -> Result<BooleanArray>>(
        lhs: Vec<&[u8]>,
        rhs: Vec<&[u8]>,
        op: F,
        expected: Vec<bool>,
    ) {
        let lhs = BinaryArray::<O>::from_slice(lhs);
        let rhs = BinaryArray::<O>::from_slice(rhs);
        let expected = BooleanArray::from_slice(expected);
        assert_eq!(op(&lhs, &rhs).unwrap(), expected);
    }

    fn test_generic_scalar<O: Offset, F: Fn(&BinaryArray<O>, &[u8]) -> BooleanArray>(
        lhs: Vec<&[u8]>,
        rhs: &[u8],
        op: F,
        expected: Vec<bool>,
    ) {
        let lhs = BinaryArray::<O>::from_slice(lhs);
        let expected = BooleanArray::from_slice(expected);
        assert_eq!(op(&lhs, rhs), expected);
    }

    #[test]
    fn test_gt_eq() {
        test_generic::<i32, _>(
            vec![b"arrow", b"datafusion", b"flight", b"parquet"],
            vec![b"flight", b"flight", b"flight", b"flight"],
            gt_eq,
            vec![false, false, true, true],
        )
    }

    #[test]
    fn test_gt_eq_scalar() {
        test_generic_scalar::<i32, _>(
            vec![b"arrow", b"datafusion", b"flight", b"parquet"],
            b"flight",
            gt_eq_scalar,
            vec![false, false, true, true],
        )
    }

    #[test]
    fn test_eq() {
        test_generic::<i32, _>(
            vec![b"arrow", b"arrow", b"arrow", b"arrow"],
            vec![b"arrow", b"parquet", b"datafusion", b"flight"],
            eq,
            vec![true, false, false, false],
        )
    }

    #[test]
    fn test_eq_scalar() {
        test_generic_scalar::<i32, _>(
            vec![b"arrow", b"parquet", b"datafusion", b"flight"],
            b"arrow",
            eq_scalar,
            vec![true, false, false, false],
        )
    }

    #[test]
    fn test_neq() {
        test_generic::<i32, _>(
            vec![b"arrow", b"arrow", b"arrow", b"arrow"],
            vec![b"arrow", b"parquet", b"datafusion", b"flight"],
            neq,
            vec![false, true, true, true],
        )
    }

    #[test]
    fn test_neq_scalar() {
        test_generic_scalar::<i32, _>(
            vec![b"arrow", b"parquet", b"datafusion", b"flight"],
            b"arrow",
            neq_scalar,
            vec![false, true, true, true],
        )
    }
}
