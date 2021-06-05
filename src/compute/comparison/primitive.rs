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

use crate::bitmap::Bitmap;
use crate::{array::*, types::NativeType};
use crate::{
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
    error::{ArrowError, Result},
};

use super::{super::utils::combine_validities, Operator};

pub(crate) fn compare_values_op<T, F>(lhs: &[T], rhs: &[T], op: F) -> MutableBitmap
where
    T: NativeType,
    F: Fn(T, T) -> bool,
{
    assert_eq!(lhs.len(), rhs.len());
    let mut values = MutableBuffer::from_len_zeroed((lhs.len() + 7) / 8);

    let lhs_chunks_iter = lhs.chunks_exact(8);
    let lhs_remainder = lhs_chunks_iter.remainder();
    let rhs_chunks_iter = rhs.chunks_exact(8);
    let rhs_remainder = rhs_chunks_iter.remainder();

    let chunks = lhs.len() / 8;

    values[..chunks]
        .iter_mut()
        .zip(lhs_chunks_iter)
        .zip(rhs_chunks_iter)
        .for_each(|((byte, lhs), rhs)| {
            lhs.iter()
                .zip(rhs.iter())
                .enumerate()
                .for_each(|(i, (&lhs, &rhs))| {
                    *byte |= if op(lhs, rhs) { 1 << i } else { 0 };
                });
        });

    if !lhs_remainder.is_empty() {
        let last = &mut values[chunks];
        lhs_remainder
            .iter()
            .zip(rhs_remainder.iter())
            .enumerate()
            .for_each(|(i, (&lhs, &rhs))| {
                *last |= if op(lhs, rhs) { 1 << i } else { 0 };
            });
    };
    (values, lhs.len()).into()
}

/// Evaluate `op(lhs, rhs)` for [`PrimitiveArray`]s using a specified
/// comparison function.
fn compare_op<T, F>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>, op: F) -> Result<BooleanArray>
where
    T: NativeType,
    F: Fn(T, T) -> bool,
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

/// Evaluate `op(left, right)` for [`PrimitiveArray`] and scalar using
/// a specified comparison function.
pub fn compare_op_scalar<T, F>(lhs: &PrimitiveArray<T>, rhs: T, op: F) -> Result<BooleanArray>
where
    T: NativeType,
    F: Fn(T, T) -> bool,
{
    let validity = lhs.validity().clone();

    let mut values = MutableBuffer::from_len_zeroed((lhs.len() + 7) / 8);

    let lhs_chunks_iter = lhs.values().chunks_exact(8);
    let lhs_remainder = lhs_chunks_iter.remainder();
    let chunks = lhs.len() / 8;

    values[..chunks]
        .iter_mut()
        .zip(lhs_chunks_iter)
        .for_each(|(byte, chunk)| {
            chunk.iter().enumerate().for_each(|(i, &c_i)| {
                *byte |= if op(c_i, rhs) { 1 << i } else { 0 };
            });
        });

    if !lhs_remainder.is_empty() {
        let last = &mut values[chunks];
        lhs_remainder.iter().enumerate().for_each(|(i, &lhs)| {
            *last |= if op(lhs, rhs) { 1 << i } else { 0 };
        });
    };

    Ok(BooleanArray::from_data(
        Bitmap::from_u8_buffer(values, lhs.len()),
        validity,
    ))
}

/// Perform `lhs == rhs` operation on two arrays.
pub fn eq<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: NativeType,
{
    compare_op(lhs, rhs, |a, b| a == b)
}

/// Perform `left == right` operation on an array and a scalar value.
pub fn eq_scalar<T>(lhs: &PrimitiveArray<T>, rhs: T) -> Result<BooleanArray>
where
    T: NativeType,
{
    compare_op_scalar(lhs, rhs, |a, b| a == b)
}

/// Perform `left != right` operation on two arrays.
pub fn neq<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: NativeType,
{
    compare_op(lhs, rhs, |a, b| a != b)
}

/// Perform `left != right` operation on an array and a scalar value.
pub fn neq_scalar<T>(lhs: &PrimitiveArray<T>, rhs: T) -> Result<BooleanArray>
where
    T: NativeType,
{
    compare_op_scalar(lhs, rhs, |a, b| a != b)
}

/// Perform `left < right` operation on two arrays.
pub fn lt<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: NativeType + std::cmp::PartialOrd,
{
    compare_op(lhs, rhs, |a, b| a < b)
}

/// Perform `left < right` operation on an array and a scalar value.
pub fn lt_scalar<T>(lhs: &PrimitiveArray<T>, rhs: T) -> Result<BooleanArray>
where
    T: NativeType + std::cmp::PartialOrd,
{
    compare_op_scalar(lhs, rhs, |a, b| a < b)
}

/// Perform `left <= right` operation on two arrays.
pub fn lt_eq<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: NativeType + std::cmp::PartialOrd,
{
    compare_op(lhs, rhs, |a, b| a <= b)
}

/// Perform `left <= right` operation on an array and a scalar value.
/// Null values are less than non-null values.
pub fn lt_eq_scalar<T>(lhs: &PrimitiveArray<T>, rhs: T) -> Result<BooleanArray>
where
    T: NativeType + std::cmp::PartialOrd,
{
    compare_op_scalar(lhs, rhs, |a, b| a <= b)
}

/// Perform `left > right` operation on two arrays. Non-null values are greater than null
/// values.
pub fn gt<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: NativeType + std::cmp::PartialOrd,
{
    compare_op(lhs, rhs, |a, b| a > b)
}

/// Perform `left > right` operation on an array and a scalar value.
/// Non-null values are greater than null values.
pub fn gt_scalar<T>(lhs: &PrimitiveArray<T>, rhs: T) -> Result<BooleanArray>
where
    T: NativeType + std::cmp::PartialOrd,
{
    compare_op_scalar(lhs, rhs, |a, b| a > b)
}

/// Perform `left >= right` operation on two arrays. Non-null values are greater than null
/// values.
pub fn gt_eq<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: NativeType + std::cmp::PartialOrd,
{
    compare_op(lhs, rhs, |a, b| a >= b)
}

/// Perform `left >= right` operation on an array and a scalar value.
/// Non-null values are greater than null values.
pub fn gt_eq_scalar<T>(lhs: &PrimitiveArray<T>, rhs: T) -> Result<BooleanArray>
where
    T: NativeType + std::cmp::PartialOrd,
{
    compare_op_scalar(lhs, rhs, |a, b| a >= b)
}

pub fn compare<T: NativeType + std::cmp::PartialOrd>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
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

pub fn compare_scalar<T: NativeType + std::cmp::PartialOrd>(
    lhs: &PrimitiveArray<T>,
    rhs: T,
    op: Operator,
) -> Result<BooleanArray> {
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
    use crate::datatypes::DataType;

    /// Evaluate `KERNEL` with two vectors as inputs and assert against the expected output.
    /// `A_VEC` and `B_VEC` can be of type `Vec<i64>` or `Vec<Option<i64>>`.
    /// `EXPECTED` can be either `Vec<bool>` or `Vec<Option<bool>>`.
    /// The main reason for this macro is that inputs and outputs align nicely after `cargo fmt`.
    macro_rules! cmp_i64 {
        ($KERNEL:ident, $A_VEC:expr, $B_VEC:expr, $EXPECTED:expr) => {
            let a = Int64Array::from_slice($A_VEC);
            let b = Int64Array::from_slice($B_VEC);
            let c = $KERNEL(&a, &b).unwrap();
            assert_eq!(BooleanArray::from_slice($EXPECTED), c);
        };
    }

    macro_rules! cmp_i64_options {
        ($KERNEL:ident, $A_VEC:expr, $B_VEC:expr, $EXPECTED:expr) => {
            let a = Int64Array::from($A_VEC);
            let b = Int64Array::from($B_VEC);
            let c = $KERNEL(&a, &b).unwrap();
            assert_eq!(BooleanArray::from($EXPECTED), c);
        };
    }

    /// Evaluate `KERNEL` with one vectors and one scalar as inputs and assert against the expected output.
    /// `A_VEC` can be of type `Vec<i64>` or `Vec<Option<i64>>`.
    /// `EXPECTED` can be either `Vec<bool>` or `Vec<Option<bool>>`.
    /// The main reason for this macro is that inputs and outputs align nicely after `cargo fmt`.
    macro_rules! cmp_i64_scalar_options {
        ($KERNEL:ident, $A_VEC:expr, $B:literal, $EXPECTED:expr) => {
            let a = Int64Array::from($A_VEC);
            let c = $KERNEL(&a, $B).unwrap();
            assert_eq!(BooleanArray::from($EXPECTED), c);
        };
    }

    macro_rules! cmp_i64_scalar {
        ($KERNEL:ident, $A_VEC:expr, $B:literal, $EXPECTED:expr) => {
            let a = Int64Array::from_slice($A_VEC);
            let c = $KERNEL(&a, $B).unwrap();
            assert_eq!(BooleanArray::from_slice($EXPECTED), c);
        };
    }

    #[test]
    fn test_primitive_array_eq() {
        cmp_i64!(
            eq,
            &[8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            &[6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, true, false, false, false, false, true, false, false]
        );
    }

    #[test]
    fn test_primitive_array_eq_scalar() {
        cmp_i64_scalar!(
            eq_scalar,
            &[6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![false, false, true, false, false, false, false, true, false, false]
        );
    }

    #[test]
    fn test_primitive_array_eq_with_slice() {
        let a = Int64Array::from_slice(&[6, 7, 8, 8, 10]);
        let b = Int64Array::from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let c = b.slice(5, 5);
        let d = eq(&c, &a).unwrap();
        assert_eq!(
            d,
            BooleanArray::from_slice(&vec![true, true, true, false, true])
        );
    }

    #[test]
    fn test_primitive_array_neq() {
        cmp_i64!(
            neq,
            &[8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            &[6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![true, true, false, true, true, true, true, false, true, true]
        );
    }

    #[test]
    fn test_primitive_array_neq_scalar() {
        cmp_i64_scalar!(
            neq_scalar,
            &[6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![true, true, false, true, true, true, true, false, true, true]
        );
    }

    #[test]
    fn test_primitive_array_lt() {
        cmp_i64!(
            lt,
            &[8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            &[6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, false, true, true, false, false, false, true, true]
        );
    }

    #[test]
    fn test_primitive_array_lt_scalar() {
        cmp_i64_scalar!(
            lt_scalar,
            &[6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![true, true, false, false, false, true, true, false, false, false]
        );
    }

    #[test]
    fn test_primitive_array_lt_nulls() {
        cmp_i64_options!(
            lt,
            &[None, None, Some(1), Some(1), None, None, Some(2), Some(2),],
            &[None, Some(1), None, Some(1), None, Some(3), None, Some(3),],
            vec![None, None, None, Some(false), None, None, None, Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_lt_scalar_nulls() {
        cmp_i64_scalar_options!(
            lt_scalar,
            &[None, Some(1), Some(2), Some(3), None, Some(1), Some(2), Some(3), Some(2), None],
            2,
            vec![None, Some(true), Some(false), Some(false), None, Some(true), Some(false), Some(false), Some(false), None]
        );
    }

    #[test]
    fn test_primitive_array_lt_eq() {
        cmp_i64!(
            lt_eq,
            &[8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            &[6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![false, false, true, true, true, false, false, true, true, true]
        );
    }

    #[test]
    fn test_primitive_array_lt_eq_scalar() {
        cmp_i64_scalar!(
            lt_eq_scalar,
            &[6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![true, true, true, false, false, true, true, true, false, false]
        );
    }

    #[test]
    fn test_primitive_array_lt_eq_nulls() {
        cmp_i64_options!(
            lt_eq,
            &[
                None,
                None,
                Some(1),
                None,
                None,
                Some(1),
                None,
                None,
                Some(1)
            ],
            &[
                None,
                Some(1),
                Some(0),
                None,
                Some(1),
                Some(2),
                None,
                None,
                Some(3)
            ],
            vec![None, None, Some(false), None, None, Some(true), None, None, Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_lt_eq_scalar_nulls() {
        cmp_i64_scalar_options!(
            lt_eq_scalar,
            &[None, Some(1), Some(2), None, Some(1), Some(2), None, Some(1), Some(2)],
            1,
            vec![None, Some(true), Some(false), None, Some(true), Some(false), None, Some(true), Some(false)]
        );
    }

    #[test]
    fn test_primitive_array_gt() {
        cmp_i64!(
            gt,
            &[8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            &[6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![true, true, false, false, false, true, true, false, false, false]
        );
    }

    #[test]
    fn test_primitive_array_gt_scalar() {
        cmp_i64_scalar!(
            gt_scalar,
            &[6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![false, false, false, true, true, false, false, false, true, true]
        );
    }

    #[test]
    fn test_primitive_array_gt_nulls() {
        cmp_i64_options!(
            gt,
            &[
                None,
                None,
                Some(1),
                None,
                None,
                Some(2),
                None,
                None,
                Some(3)
            ],
            &[
                None,
                Some(1),
                Some(1),
                None,
                Some(1),
                Some(1),
                None,
                Some(1),
                Some(1)
            ],
            vec![None, None, Some(false), None, None, Some(true), None, None, Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_gt_scalar_nulls() {
        cmp_i64_scalar_options!(
            gt_scalar,
            &[None, Some(1), Some(2), None, Some(1), Some(2), None, Some(1), Some(2)],
            1,
            vec![None, Some(false), Some(true), None, Some(false), Some(true), None, Some(false), Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_gt_eq() {
        cmp_i64!(
            gt_eq,
            &[8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
            &[6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            vec![true, true, true, false, false, true, true, true, false, false]
        );
    }

    #[test]
    fn test_primitive_array_gt_eq_scalar() {
        cmp_i64_scalar!(
            gt_eq_scalar,
            &[6, 7, 8, 9, 10, 6, 7, 8, 9, 10],
            8,
            vec![false, false, true, true, true, false, false, true, true, true]
        );
    }

    #[test]
    fn test_primitive_array_gt_eq_nulls() {
        cmp_i64_options!(
            gt_eq,
            vec![None, None, Some(1), None, Some(1), Some(2), None, None, Some(1)],
            vec![None, Some(1), None, None, Some(1), Some(1), None, Some(2), Some(2)],
            vec![None, None, None, None, Some(true), Some(true), None, None, Some(false)]
        );
    }

    #[test]
    fn test_primitive_array_gt_eq_scalar_nulls() {
        cmp_i64_scalar_options!(
            gt_eq_scalar,
            vec![None, Some(1), Some(2), None, Some(2), Some(3), None, Some(3), Some(4)],
            2,
            vec![None, Some(false), Some(true), None, Some(true), Some(true), None, Some(true), Some(true)]
        );
    }

    #[test]
    fn test_primitive_array_compare_slice() {
        let a = (0..100)
            .map(Some)
            .collect::<Primitive<i32>>()
            .to(DataType::Int32);
        let a = a.slice(50, 50);
        let b = (100..200)
            .map(Some)
            .collect::<Primitive<i32>>()
            .to(DataType::Int32);
        let b = b.slice(50, 50);
        let actual = lt(&a, &b).unwrap();
        let expected: BooleanArray = (0..50).map(|_| Some(true)).collect();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_primitive_array_compare_scalar_slice() {
        let a = (0..100)
            .map(Some)
            .collect::<Primitive<i32>>()
            .to(DataType::Int32);
        let a = a.slice(50, 50);
        let actual = lt_scalar(&a, 200).unwrap();
        let expected: BooleanArray = (0..50).map(|_| Some(true)).collect();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_length_of_result_buffer() {
        // `item_count` is chosen to not be a multiple of 64.
        let item_count = 130;

        let array_a = Int8Array::from_slice(&vec![1; item_count]);
        let array_b = Int8Array::from_slice(&vec![2; item_count]);
        let expected = BooleanArray::from_slice(&vec![false; item_count]);
        let result = gt_eq(&array_a, &array_b).unwrap();

        assert_eq!(result, expected)
    }
}
