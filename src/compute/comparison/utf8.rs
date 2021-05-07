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

use crate::error::{ArrowError, Result};
use crate::{array::*, bitmap::Bitmap};

use super::{super::utils::combine_validities, Operator};

/// Evaluate `op(lhs, rhs)` for [`PrimitiveArray`]s using a specified
/// comparison function.
fn compare_op<O, F>(lhs: &Utf8Array<O>, rhs: &Utf8Array<O>, op: F) -> Result<BooleanArray>
where
    O: Offset,
    F: Fn(&str, &str) -> bool,
{
    if lhs.len() != rhs.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Cannot perform comparison operation on arrays of different length".to_string(),
        ));
    }

    let validity = combine_validities(lhs.validity(), rhs.validity());

    let values = lhs
        .iter()
        .zip(rhs.iter())
        .map(|(lhs, rhs)| match (lhs, rhs) {
            (Some(lhs), Some(rhs)) => op(lhs, rhs),
            _ => false,
        });
    let values = Bitmap::from_trusted_len_iter(values);

    Ok(BooleanArray::from_data(values, validity))
}

/// Evaluate `op(lhs, rhs)` for [`PrimitiveArray`] and scalar using
/// a specified comparison function.
fn compare_op_scalar<O, F>(lhs: &Utf8Array<O>, rhs: &str, op: F) -> BooleanArray
where
    O: Offset,
    F: Fn(&str, &str) -> bool,
{
    let validity = lhs.validity().clone();

    let values = lhs.iter().map(|lhs| match lhs {
        None => false,
        Some(lhs) => op(lhs, rhs),
    });
    let values = Bitmap::from_trusted_len_iter(values);

    BooleanArray::from_data(values, validity)
}

/// Perform `lhs == rhs` operation on [`StringArray`] / [`LargeStringArray`].
fn eq<O: Offset>(lhs: &Utf8Array<O>, rhs: &Utf8Array<O>) -> Result<BooleanArray> {
    compare_op(lhs, rhs, |a, b| a == b)
}

/// Perform `lhs == rhs` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
fn eq_scalar<O: Offset>(lhs: &Utf8Array<O>, rhs: &str) -> BooleanArray {
    compare_op_scalar(lhs, rhs, |a, b| a == b)
}

/// Perform `lhs != rhs` operation on [`StringArray`] / [`LargeStringArray`].
fn neq<O: Offset>(lhs: &Utf8Array<O>, rhs: &Utf8Array<O>) -> Result<BooleanArray> {
    compare_op(lhs, rhs, |a, b| a != b)
}

/// Perform `lhs != rhs` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
fn neq_scalar<O: Offset>(lhs: &Utf8Array<O>, rhs: &str) -> BooleanArray {
    compare_op_scalar(lhs, rhs, |a, b| a != b)
}

/// Perform `lhs < rhs` operation on [`StringArray`] / [`LargeStringArray`].
fn lt<O: Offset>(lhs: &Utf8Array<O>, rhs: &Utf8Array<O>) -> Result<BooleanArray> {
    compare_op(lhs, rhs, |a, b| a < b)
}

/// Perform `lhs < rhs` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
fn lt_scalar<O: Offset>(lhs: &Utf8Array<O>, rhs: &str) -> BooleanArray {
    compare_op_scalar(lhs, rhs, |a, b| a < b)
}

/// Perform `lhs <= rhs` operation on [`StringArray`] / [`LargeStringArray`].
fn lt_eq<O: Offset>(lhs: &Utf8Array<O>, rhs: &Utf8Array<O>) -> Result<BooleanArray> {
    compare_op(lhs, rhs, |a, b| a <= b)
}

/// Perform `lhs <= rhs` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
fn lt_eq_scalar<O: Offset>(lhs: &Utf8Array<O>, rhs: &str) -> BooleanArray {
    compare_op_scalar(lhs, rhs, |a, b| a <= b)
}

/// Perform `lhs > rhs` operation on [`StringArray`] / [`LargeStringArray`].
fn gt<O: Offset>(lhs: &Utf8Array<O>, rhs: &Utf8Array<O>) -> Result<BooleanArray> {
    compare_op(lhs, rhs, |a, b| a > b)
}

/// Perform `lhs > rhs` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
fn gt_scalar<O: Offset>(lhs: &Utf8Array<O>, rhs: &str) -> BooleanArray {
    compare_op_scalar(lhs, rhs, |a, b| a > b)
}

/// Perform `lhs >= rhs` operation on [`StringArray`] / [`LargeStringArray`].
fn gt_eq<O: Offset>(lhs: &Utf8Array<O>, rhs: &Utf8Array<O>) -> Result<BooleanArray> {
    compare_op(lhs, rhs, |a, b| a >= b)
}

/// Perform `lhs >= rhs` operation on [`StringArray`] / [`LargeStringArray`] and a scalar.
fn gt_eq_scalar<O: Offset>(lhs: &Utf8Array<O>, rhs: &str) -> BooleanArray {
    compare_op_scalar(lhs, rhs, |a, b| a >= b)
}

pub fn compare<O: Offset>(
    lhs: &Utf8Array<O>,
    rhs: &Utf8Array<O>,
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

pub fn compare_scalar<O: Offset>(lhs: &Utf8Array<O>, rhs: &str, op: Operator) -> BooleanArray {
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

    fn test_generic<O: Offset, F: Fn(&Utf8Array<O>, &Utf8Array<O>) -> Result<BooleanArray>>(
        lhs: Vec<&str>,
        rhs: Vec<&str>,
        op: F,
        expected: Vec<bool>,
    ) {
        let lhs = Utf8Array::<O>::from_slice(lhs);
        let rhs = Utf8Array::<O>::from_slice(rhs);
        let expected = BooleanArray::from_slice(expected);
        assert_eq!(op(&lhs, &rhs).unwrap(), expected);
    }

    fn test_generic_scalar<O: Offset, F: Fn(&Utf8Array<O>, &str) -> BooleanArray>(
        lhs: Vec<&str>,
        rhs: &str,
        op: F,
        expected: Vec<bool>,
    ) {
        let lhs = Utf8Array::<O>::from_slice(lhs);
        let expected = BooleanArray::from_slice(expected);
        assert_eq!(op(&lhs, rhs), expected);
    }

    #[test]
    fn test_gt_eq() {
        test_generic::<i32, _>(
            vec!["arrow", "datafusion", "flight", "parquet"],
            vec!["flight", "flight", "flight", "flight"],
            gt_eq,
            vec![false, false, true, true],
        )
    }

    #[test]
    fn test_gt_eq_scalar() {
        test_generic_scalar::<i32, _>(
            vec!["arrow", "datafusion", "flight", "parquet"],
            "flight",
            gt_eq_scalar,
            vec![false, false, true, true],
        )
    }

    #[test]
    fn test_eq() {
        test_generic::<i32, _>(
            vec!["arrow", "arrow", "arrow", "arrow"],
            vec!["arrow", "parquet", "datafusion", "flight"],
            eq,
            vec![true, false, false, false],
        )
    }

    #[test]
    fn test_eq_scalar() {
        test_generic_scalar::<i32, _>(
            vec!["arrow", "parquet", "datafusion", "flight"],
            "arrow",
            eq_scalar,
            vec![true, false, false, false],
        )
    }

    #[test]
    fn test_neq() {
        test_generic::<i32, _>(
            vec!["arrow", "arrow", "arrow", "arrow"],
            vec!["arrow", "parquet", "datafusion", "flight"],
            neq,
            vec![false, true, true, true],
        )
    }

    #[test]
    fn test_neq_scalar() {
        test_generic_scalar::<i32, _>(
            vec!["arrow", "parquet", "datafusion", "flight"],
            "arrow",
            neq_scalar,
            vec![false, true, true, true],
        )
    }

    /*
    test_utf8!(
        test_utf8_array_lt,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        lt_utf8,
        vec![true, true, false, false]
    );
    test_utf8_scalar!(
        test_utf8_array_lt_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        lt_utf8_scalar,
        vec![true, true, false, false]
    );

    test_utf8!(
        test_utf8_array_lt_eq,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        lt_eq_utf8,
        vec![true, true, true, false]
    );
    test_utf8_scalar!(
        test_utf8_array_lt_eq_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        lt_eq_utf8_scalar,
        vec![true, true, true, false]
    );

    test_utf8!(
        test_utf8_array_gt,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        gt_utf8,
        vec![false, false, false, true]
    );
    test_utf8_scalar!(
        test_utf8_array_gt_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        gt_utf8_scalar,
        vec![false, false, false, true]
    );

    test_utf8!(
        test_utf8_array_gt_eq,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        gt_eq_utf8,
        vec![false, false, true, true]
    );
    test_utf8_scalar!(
        test_utf8_array_gt_eq_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        gt_eq_utf8_scalar,
        vec![false, false, true, true]
    );
    */
}
