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

//! Defines aggregations over Arrow arrays.

use std::{iter::Sum, ops::AddAssign};

use crate::types::{BitChunkIter, NativeType};
use crate::{
    array::{ord::*, Array, BooleanArray, Offset, PrimitiveArray, Utf8Array},
    bitmap::Bitmap,
};
use multiversion::multiversion;

/// Helper macro to perform min/max of strings
fn min_max_string<O: Offset, F: Fn(&str, &str) -> bool>(
    array: &Utf8Array<O>,
    cmp: F,
) -> Option<&str> {
    let null_count = array.null_count();

    if null_count == array.len() || array.len() == 0 {
        return None;
    }
    let mut n;
    if let Some(validity) = array.validity() {
        n = "";
        let mut has_value = false;

        for i in 0..array.len() {
            let item = array.value(i);
            if validity.get_bit(i) && (!has_value || cmp(&n, item)) {
                has_value = true;
                n = item;
            }
        }
    } else {
        // array.len() == 0 checked above
        n = unsafe { array.value_unchecked(0) };
        for i in 1..array.len() {
            // loop is up to `len`.
            let item = unsafe { array.value_unchecked(i) };
            if cmp(&n, item) {
                n = item;
            }
        }
    }
    Some(n)
}

/// Returns the minimum value in the array, according to the natural order.
/// For floating point arrays any NaN values are considered to be greater than any other non-null value
pub fn min<T>(array: &PrimitiveArray<T>) -> Option<T>
where
    T: NativeType + Ord,
{
    min_max_helper(array, total_cmp)
}

pub fn max_f32(array: &PrimitiveArray<f32>) -> Option<f32> {
    min_max_helper(array, |x, y| total_cmp_f32(x, y).reverse())
}

pub fn max_f64(array: &PrimitiveArray<f64>) -> Option<f64> {
    min_max_helper(array, |x, y| total_cmp_f64(x, y).reverse())
}

pub fn min_f32(array: &PrimitiveArray<f32>) -> Option<f32> {
    min_max_helper(array, |x, y| total_cmp_f32(x, y))
}

pub fn min_f64(array: &PrimitiveArray<f64>) -> Option<f64> {
    min_max_helper(array, |x, y| total_cmp_f64(x, y))
}

/// Returns the maximum value in the array, according to the natural order.
/// For floating point arrays any NaN values are considered to be greater than any other non-null value
pub fn max<T>(array: &PrimitiveArray<T>) -> Option<T>
where
    T: NativeType + Ord,
{
    min_max_helper(array, |x, y| total_cmp(x, y).reverse())
}

/// Returns the maximum value in the string array, according to the natural order.
pub fn max_string<O: Offset>(array: &Utf8Array<O>) -> Option<&str> {
    min_max_string(array, |a, b| a < b)
}

/// Returns the minimum value in the string array, according to the natural order.
pub fn min_string<O: Offset>(array: &Utf8Array<O>) -> Option<&str> {
    min_max_string(array, |a, b| a > b)
}

#[inline]
fn reduce_slice<T, F>(initial: T, values: &[T], cmp: F) -> T
where
    T: NativeType,
    F: Fn(&T, &T) -> std::cmp::Ordering,
{
    values.iter().fold(initial, |max, item| {
        if cmp(&max, item) == std::cmp::Ordering::Greater {
            *item
        } else {
            max
        }
    })
}

fn nonnull_min_max_primitive<T, F>(values: &[T], cmp: F) -> T
where
    T: NativeType,
    F: Fn(&T, &T) -> std::cmp::Ordering,
{
    if values.len() < T::LANES {
        return reduce_slice(values[0], &values[1..], cmp);
    };
    let mut chunks = values.chunks_exact(T::LANES);
    let remainder = chunks.remainder();

    let initial = T::from_slice(chunks.next().unwrap());

    let chunk_reduced = chunks.fold(initial, |mut acc, chunk| {
        let chunk = T::from_slice(chunk);
        for i in 0..T::LANES {
            if cmp(&acc[i], &chunk[i]) == std::cmp::Ordering::Greater {
                acc[i] = chunk[i];
            }
        }
        acc
    });

    let mut reduced = chunk_reduced[0];
    for i in 1..T::LANES {
        if cmp(&reduced, &chunk_reduced[i]) == std::cmp::Ordering::Greater {
            reduced = chunk_reduced[i];
        }
    }

    reduce_slice(reduced, remainder, cmp)
}

/// Helper function to perform min/max lambda function on values from a numeric array.
#[inline]
fn min_max_helper<T, F>(array: &PrimitiveArray<T>, cmp: F) -> Option<T>
where
    T: NativeType,
    F: Fn(&T, &T) -> std::cmp::Ordering,
{
    let null_count = array.null_count();

    // Includes case array.len() == 0
    if null_count == array.len() {
        return None;
    }
    let values = array.values();

    if let Some(validity) = array.validity() {
        let mut n = T::default();
        let mut has_value = false;
        for (i, item) in values.iter().enumerate() {
            let is_valid = unsafe { validity.get_bit_unchecked(i) };
            if is_valid && (!has_value || cmp(&n, item) == std::cmp::Ordering::Greater) {
                has_value = true;
                n = *item
            }
        }
        Some(n)
    } else {
        Some(nonnull_min_max_primitive(values, cmp))
    }
}

/// Returns the minimum value in the boolean array.
///
/// ```
/// use arrow2::{
///   array::BooleanArray,
///   compute::aggregate::min_boolean,
/// };
///
/// let a = BooleanArray::from(vec![Some(true), None, Some(false)]);
/// assert_eq!(min_boolean(&a), Some(false))
/// ```
pub fn min_boolean(array: &BooleanArray) -> Option<bool> {
    // short circuit if all nulls / zero length array
    if array.null_count() == array.len() {
        return None;
    }

    // Note the min bool is false (0), so short circuit as soon as we see it
    array
        .iter()
        .find(|&b| b == Some(false))
        .flatten()
        .or(Some(true))
}

/// Returns the maximum value in the boolean array
///
/// ```
/// use arrow2::{
///   array::BooleanArray,
///   compute::aggregate::max_boolean,
/// };
///
/// let a = BooleanArray::from(vec![Some(true), None, Some(false)]);
/// assert_eq!(max_boolean(&a), Some(true))
/// ```
pub fn max_boolean(array: &BooleanArray) -> Option<bool> {
    // short circuit if all nulls / zero length array
    if array.null_count() == array.len() {
        return None;
    }

    // Note the max bool is true (1), so short circuit as soon as we see it
    array
        .iter()
        .find(|&b| b == Some(true))
        .flatten()
        .or(Some(false))
}

#[multiversion]
#[clone(target = "x86_64+avx")]
fn nonnull_sum<T: NativeType + AddAssign + Sum>(values: &[T]) -> T {
    let chunks = values.chunks_exact(T::LANES);
    let remainder = chunks.remainder();

    let sum = chunks.fold(T::new_simd(), |mut acc, chunk| {
        let chunk = T::from_slice(chunk);
        for i in 0..T::LANES {
            acc[i] += chunk[i];
        }
        acc
    });

    let mut reduced: T = remainder.iter().copied().sum();

    for i in 0..T::LANES {
        reduced += sum[i];
    }
    reduced
}

/// # Panics
/// iff `values.len() != bitmap.len()` or the operation overflows.
#[multiversion]
#[clone(target = "x86_64+avx")]
fn null_sum<T: NativeType + AddAssign + Sum>(values: &[T], bitmap: &Bitmap) -> T {
    let mut chunks = values.chunks_exact(T::LANES);

    let mut validity_masks = bitmap.chunks::<T::SimdMask>();

    let sum = chunks.by_ref().zip(validity_masks.by_ref()).fold(
        T::new_simd(),
        |mut acc, (chunk, validity_chunk)| {
            let chunk = T::from_slice(chunk);
            let iter = BitChunkIter::new(validity_chunk, T::LANES);
            for (i, b) in (0..T::LANES).zip(iter) {
                acc[i] += if b { chunk[i] } else { T::default() };
            }
            acc
        },
    );

    let mut reduced: T = chunks
        .remainder()
        .iter()
        .zip(BitChunkIter::new(validity_masks.remainder(), T::LANES))
        .map(|(x, is_valid)| if is_valid { *x } else { T::default() })
        .sum();

    for i in 0..T::LANES {
        reduced += sum[i];
    }
    reduced
}

/// Returns the sum of values in the array.
///
/// Returns `None` if the array is empty or only contains null values.
pub fn sum<T>(array: &PrimitiveArray<T>) -> Option<T>
where
    T: NativeType + Sum + AddAssign,
{
    let null_count = array.null_count();

    if null_count == array.len() {
        return None;
    }

    match array.validity() {
        None => Some(nonnull_sum(array.values())),
        Some(bitmap) => Some(null_sum(array.values(), bitmap)),
    }
}

#[cfg(test)]
mod tests {
    use super::super::arithmetics;
    use super::*;
    use crate::array::*;
    use crate::datatypes::DataType;

    #[test]
    fn test_primitive_array_sum() {
        let a = Primitive::from_slice(&[1, 2, 3, 4, 5]).to(DataType::Int32);
        assert_eq!(15, sum(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_float_sum() {
        let a = Primitive::from_slice(&[1.1f64, 2.2, 3.3, 4.4, 5.5]).to(DataType::Float64);
        assert!((16.5 - sum(&a).unwrap()).abs() < f64::EPSILON);
    }

    #[test]
    fn test_primitive_array_sum_with_nulls() {
        let a = Primitive::from(vec![None, Some(2), Some(3), None, Some(5)]).to(DataType::Int32);
        assert_eq!(10, sum(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_sum_all_nulls() {
        let a = Primitive::<i32>::from(vec![None, None, None]).to(DataType::Int32);
        assert_eq!(None, sum(&a));
    }

    #[test]
    fn test_primitive_array_sum_large_64() {
        let a: Int64Array = (1..=100)
            .map(|i| if i % 3 == 0 { Some(i) } else { None })
            .collect::<Primitive<i64>>()
            .to(DataType::Int64);
        let b: Int64Array = (1..=100)
            .map(|i| if i % 3 == 0 { Some(0) } else { Some(i) })
            .collect::<Primitive<i64>>()
            .to(DataType::Int64);
        // create an array that actually has non-zero values at the invalid indices
        let c = arithmetics::basic::add::add(&a, &b).unwrap();
        assert_eq!(Some((1..=100).filter(|i| i % 3 == 0).sum()), sum(&c));
    }

    #[test]
    fn test_primitive_array_min_max() {
        let a = Primitive::<i32>::from_slice(&[5, 6, 7, 8, 9]).to(DataType::Int32);
        assert_eq!(5, min(&a).unwrap());
        assert_eq!(9, max(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_min_max_with_nulls() {
        let a =
            Primitive::<i32>::from(&[Some(5), None, None, Some(8), Some(9)]).to(DataType::Int32);
        assert_eq!(5, min(&a).unwrap());
        assert_eq!(9, max(&a).unwrap());
    }

    #[test]
    fn test_primitive_min_max_1() {
        let a = Primitive::<i32>::from(&[None, None, Some(5), Some(2)]).to(DataType::Int32);
        assert_eq!(Some(2), min(&a));
        assert_eq!(Some(5), max(&a));
    }

    // todo: convert me
    /*
    #[test]
    fn test_primitive_min_max_float_large_nonnull_array() {
        let a: Float64Array = (0..256).map(|i| Some((i + 1) as f64)).collect();
        // min/max are on boundaries of chunked data
        assert_eq!(Some(1.0), min(&a));
        assert_eq!(Some(256.0), max(&a));

        // max is last value in remainder after chunking
        let a: Float64Array = (0..255).map(|i| Some((i + 1) as f64)).collect();
        assert_eq!(Some(255.0), max(&a));

        // max is first value in remainder after chunking
        let a: Float64Array = (0..257).map(|i| Some((i + 1) as f64)).collect();
        assert_eq!(Some(257.0), max(&a));
    }

    #[test]
    fn test_primitive_min_max_float_large_nullable_array() {
        let a: Float64Array = (0..256)
            .map(|i| {
                if (i + 1) % 3 == 0 {
                    None
                } else {
                    Some((i + 1) as f64)
                }
            })
            .collect();
        // min/max are on boundaries of chunked data
        assert_eq!(Some(1.0), min(&a));
        assert_eq!(Some(256.0), max(&a));

        let a: Float64Array = (0..256)
            .map(|i| {
                if i == 0 || i == 255 {
                    None
                } else {
                    Some((i + 1) as f64)
                }
            })
            .collect();
        // boundaries of chunked data are null
        assert_eq!(Some(2.0), min(&a));
        assert_eq!(Some(255.0), max(&a));

        let a: Float64Array = (0..256)
            .map(|i| if i != 100 { None } else { Some((i) as f64) })
            .collect();
        // a single non-null value somewhere in the middle
        assert_eq!(Some(100.0), min(&a));
        assert_eq!(Some(100.0), max(&a));

        // max is last value in remainder after chunking
        let a: Float64Array = (0..255).map(|i| Some((i + 1) as f64)).collect();
        assert_eq!(Some(255.0), max(&a));

        // max is first value in remainder after chunking
        let a: Float64Array = (0..257).map(|i| Some((i + 1) as f64)).collect();
        assert_eq!(Some(257.0), max(&a));
    }

    #[test]
    fn test_primitive_min_max_float_edge_cases() {
        let a: Float64Array = (0..100).map(|_| Some(f64::NEG_INFINITY)).collect();
        assert_eq!(Some(f64::NEG_INFINITY), min(&a));
        assert_eq!(Some(f64::NEG_INFINITY), max(&a));

        let a: Float64Array = (0..100).map(|_| Some(f64::MIN)).collect();
        assert_eq!(Some(f64::MIN), min(&a));
        assert_eq!(Some(f64::MIN), max(&a));

        let a: Float64Array = (0..100).map(|_| Some(f64::MAX)).collect();
        assert_eq!(Some(f64::MAX), min(&a));
        assert_eq!(Some(f64::MAX), max(&a));

        let a: Float64Array = (0..100).map(|_| Some(f64::INFINITY)).collect();
        assert_eq!(Some(f64::INFINITY), min(&a));
        assert_eq!(Some(f64::INFINITY), max(&a));
    }

    #[test]
    fn test_primitive_min_max_float_all_nans_non_null() {
        let a: Float64Array = (0..100).map(|_| Some(f64::NAN)).collect();
        assert!(max(&a).unwrap().is_nan());
        assert!(min(&a).unwrap().is_nan());
    }

    #[test]
    fn test_primitive_min_max_float_first_nan_nonnull() {
        let a: Float64Array = (0..100)
            .map(|i| {
                if i == 0 {
                    Some(f64::NAN)
                } else {
                    Some(i as f64)
                }
            })
            .collect();
        assert_eq!(Some(1.0), min(&a));
        assert!(max(&a).unwrap().is_nan());
    }

    #[test]
    fn test_primitive_min_max_float_last_nan_nonnull() {
        let a: Float64Array = (0..100)
            .map(|i| {
                if i == 99 {
                    Some(f64::NAN)
                } else {
                    Some((i + 1) as f64)
                }
            })
            .collect();
        assert_eq!(Some(1.0), min(&a));
        assert!(max(&a).unwrap().is_nan());
    }

    #[test]
    fn test_primitive_min_max_float_first_nan_nullable() {
        let a: Float64Array = (0..100)
            .map(|i| {
                if i == 0 {
                    Some(f64::NAN)
                } else if i % 2 == 0 {
                    None
                } else {
                    Some(i as f64)
                }
            })
            .collect();
        assert_eq!(Some(1.0), min(&a));
        assert!(max(&a).unwrap().is_nan());
    }

    #[test]
    fn test_primitive_min_max_float_last_nan_nullable() {
        let a: Float64Array = (0..100)
            .map(|i| {
                if i == 99 {
                    Some(f64::NAN)
                } else if i % 2 == 0 {
                    None
                } else {
                    Some(i as f64)
                }
            })
            .collect();
        assert_eq!(Some(1.0), min(&a));
        assert!(max(&a).unwrap().is_nan());
    }

    #[test]
    fn test_primitive_min_max_float_inf_and_nans() {
        let a: Float64Array = (0..100)
            .map(|i| {
                let x = match i % 10 {
                    0 => f64::NEG_INFINITY,
                    1 => f64::MIN,
                    2 => f64::MAX,
                    4 => f64::INFINITY,
                    5 => f64::NAN,
                    _ => i as f64,
                };
                Some(x)
            })
            .collect();
        assert_eq!(Some(f64::NEG_INFINITY), min(&a));
        assert!(max(&a).unwrap().is_nan());
    }

    #[test]
    fn test_string_min_max_with_nulls() {
        let a = StringArray::from(vec![Some("b"), None, None, Some("a"), Some("c")]);
        assert_eq!("a", min_string(&a).unwrap());
        assert_eq!("c", max_string(&a).unwrap());
    }

    #[test]
    fn test_string_min_max_all_nulls() {
        let a = StringArray::from(vec![None, None]);
        assert_eq!(None, min_string(&a));
        assert_eq!(None, max_string(&a));
    }

    #[test]
    fn test_string_min_max_1() {
        let a = StringArray::from(vec![None, None, Some("b"), Some("a")]);
        assert_eq!(Some("a"), min_string(&a));
        assert_eq!(Some("b"), max_string(&a));
    }

    #[test]
    fn test_boolean_min_max_empty() {
        let a = BooleanArray::from(vec![] as Vec<Option<bool>>);
        assert_eq!(None, min_boolean(&a));
        assert_eq!(None, max_boolean(&a));
    }

    #[test]
    fn test_boolean_min_max_all_null() {
        let a = BooleanArray::from(vec![None, None]);
        assert_eq!(None, min_boolean(&a));
        assert_eq!(None, max_boolean(&a));
    }

    #[test]
    fn test_boolean_min_max_no_null() {
        let a = BooleanArray::from(vec![Some(true), Some(false), Some(true)]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));
    }

    #[test]
    fn test_boolean_min_max() {
        let a = BooleanArray::from(vec![Some(true), Some(true), None, Some(false), None]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));

        let a = BooleanArray::from(vec![None, Some(true), None, Some(false), None]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));

        let a = BooleanArray::from(vec![Some(false), Some(true), None, Some(false), None]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));
    }

    #[test]
    fn test_boolean_min_max_smaller() {
        let a = BooleanArray::from(vec![Some(false)]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(false), max_boolean(&a));

        let a = BooleanArray::from(vec![None, Some(false)]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(false), max_boolean(&a));

        let a = BooleanArray::from(vec![None, Some(true)]);
        assert_eq!(Some(true), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));

        let a = BooleanArray::from(vec![Some(true)]);
        assert_eq!(Some(true), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));
    }
     */
}
