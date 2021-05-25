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

use std::cmp::Ordering;

use crate::compute::take;
use crate::error::{ArrowError, Result};
use crate::{
    array::{ord, Array, PrimitiveArray},
    buffer::MutableBuffer,
    datatypes::DataType,
};

use super::{sort_to_indices, SortOptions};

type IsValid<'a> = Box<dyn Fn(usize) -> bool + 'a>;

/// One column to be used in lexicographical sort
#[derive(Clone, Debug)]
pub struct SortColumn<'a> {
    pub values: &'a dyn Array,
    pub options: Option<SortOptions>,
}

/// Sort a list of [`Array`] using [`SortOptions`] provided for each array.
/// # Implementaqtion
/// The sort is stable and lexicographical on values.
///
/// Returns an [`ArrowError`] if any of the array type is either unsupported by
/// `lexsort_to_indices` or `take`.
///
/// Example:
///
/// ```
/// use std::convert::From;
/// use arrow2::array::{Utf8Array, Int64Array, Array};
/// use arrow2::compute::sort::{SortColumn, SortOptions, lexsort};
/// use arrow2::datatypes::DataType;
///
/// let int64 = Int64Array::from(&[None, Some(-2), Some(89), Some(-64), Some(101)]);
/// let utf8 = Utf8Array::<i32>::from(&vec![Some("hello"), Some("world"), Some(","), Some("foobar"), Some("!")]);
///
/// let sorted_columns = lexsort(&vec![
///     SortColumn {
///         values: &int64,
///         options: None,
///     },
///     SortColumn {
///         values: &utf8,
///         options: Some(SortOptions {
///             descending: true,
///             nulls_first: false,
///         }),
///     },
/// ]).unwrap();
///
/// let sorted = sorted_columns[0].as_any().downcast_ref::<Int64Array>().unwrap();
/// assert_eq!(sorted.value(1), -64);
/// assert!(sorted.is_null(0));
/// ```
pub fn lexsort(columns: &[SortColumn]) -> Result<Vec<Box<dyn Array>>> {
    let indices = lexsort_to_indices(columns)?;
    columns
        .iter()
        .map(|c| take::take(c.values, &indices))
        .collect()
}

#[inline]
fn build_is_valid(array: &dyn Array) -> IsValid {
    if let Some(validity) = array.validity() {
        Box::new(move |x| unsafe { validity.get_bit_unchecked(x) })
    } else {
        Box::new(move |_| true)
    }
}

type Compare<'a> = Box<dyn Fn(usize, usize) -> Ordering + 'a>;

fn build_compare(array: &dyn Array, sort_option: SortOptions) -> Result<Compare> {
    let is_valid = build_is_valid(array);
    let comparator = ord::build_compare(array, array)?;

    Ok(match (sort_option.descending, sort_option.nulls_first) {
        (true, true) => Box::new(move |i: usize, j: usize| match (is_valid(i), is_valid(j)) {
            (true, true) => match (comparator)(i, j) {
                Ordering::Equal => Ordering::Equal,
                other => other.reverse(),
            },
            (false, true) => Ordering::Less,
            (true, false) => Ordering::Greater,
            (false, false) => Ordering::Equal,
        }),
        (false, true) => Box::new(move |i: usize, j: usize| match (is_valid(i), is_valid(j)) {
            (true, true) => match (comparator)(i, j) {
                Ordering::Equal => Ordering::Equal,
                other => other,
            },
            (false, true) => Ordering::Less,
            (true, false) => Ordering::Greater,
            (false, false) => Ordering::Equal,
        }),
        (false, false) => Box::new(move |i: usize, j: usize| match (is_valid(i), is_valid(j)) {
            (true, true) => match (comparator)(i, j) {
                Ordering::Equal => Ordering::Equal,
                other => other,
            },
            (false, true) => Ordering::Greater,
            (true, false) => Ordering::Less,
            (false, false) => Ordering::Equal,
        }),
        (true, false) => Box::new(move |i: usize, j: usize| match (is_valid(i), is_valid(j)) {
            (true, true) => match (comparator)(i, j) {
                Ordering::Equal => Ordering::Equal,
                other => other.reverse(),
            },
            (false, true) => Ordering::Greater,
            (true, false) => Ordering::Less,
            (false, false) => Ordering::Equal,
        }),
    })
}

/// Sort elements lexicographically from a list of `ArrayRef` into an unsigned integer
/// [`Int32Array`] of indices.
pub fn lexsort_to_indices(columns: &[SortColumn]) -> Result<PrimitiveArray<i32>> {
    if columns.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Sort requires at least one column".to_string(),
        ));
    }
    if columns.len() == 1 {
        // fallback to non-lexical sort
        let column = &columns[0];
        return sort_to_indices(column.values, &column.options.unwrap_or_default());
    }

    let row_count = columns[0].values.len();
    if columns.iter().any(|item| item.values.len() != row_count) {
        return Err(ArrowError::InvalidArgumentError(
            "lexical sort columns have different row counts".to_string(),
        ));
    };

    // map arrays to comparators
    let comparators = columns
        .iter()
        .map(|column| -> Result<Compare> {
            build_compare(column.values, column.options.unwrap_or_default())
        })
        .collect::<Result<Vec<Compare>>>()?;

    let lex_comparator = |a_idx: &i32, b_idx: &i32| -> Ordering {
        let a_idx = *a_idx as usize;
        let b_idx = *b_idx as usize;
        for comparator in comparators.iter() {
            match comparator(a_idx, b_idx) {
                Ordering::Equal => continue,
                other => return other,
            }
        }

        Ordering::Equal
    };

    // Safety: `0..row_count` is TrustedLen
    let mut values =
        unsafe { MutableBuffer::<i32>::from_trusted_len_iter_unchecked(0..row_count as i32) };
    values.sort_unstable_by(lex_comparator);

    Ok(PrimitiveArray::<i32>::from_data(
        DataType::Int32,
        values.into(),
        None,
    ))
}

#[cfg(test)]
mod tests {
    use crate::array::*;

    use super::*;

    fn test_lex_sort_arrays(input: Vec<SortColumn>, expected: Vec<Box<dyn Array>>) {
        let sorted = lexsort(&input).unwrap();
        assert_eq!(sorted, expected);
    }

    #[test]
    fn test_lex_sort_mixed_types() {
        let c1 = Int64Array::from(&[Some(0), Some(2), Some(-1), Some(0)]);
        let c2 = UInt32Array::from(&[Some(101), Some(8), Some(7), Some(102)]);
        let c3 = Int64Array::from(&[Some(-1), Some(-2), Some(-3), Some(-4)]);

        let input = vec![
            SortColumn {
                values: &c1,
                options: None,
            },
            SortColumn {
                values: &c2,
                options: None,
            },
            SortColumn {
                values: &c3,
                options: None,
            },
        ];
        let c1 = Int64Array::from(&[Some(-1), Some(0), Some(0), Some(2)]);
        let c2 = UInt32Array::from(&[Some(7), Some(101), Some(102), Some(8)]);
        let c3 = Int64Array::from(&[Some(-3), Some(-1), Some(-4), Some(-2)]);
        let expected = vec![Box::new(c1) as Box<dyn Array>, Box::new(c2), Box::new(c3)];
        test_lex_sort_arrays(input, expected);
    }

    #[test]
    fn test_lex_sort_mixed_types2() {
        // test mix of string and in64 with option
        let c1 = Int64Array::from(&[Some(0), Some(2), Some(-1), Some(0)]);
        let c2 = Utf8Array::<i32>::from(&vec![Some("foo"), Some("9"), Some("7"), Some("bar")]);
        let input = vec![
            SortColumn {
                values: &c1,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
            SortColumn {
                values: &c2,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
        ];
        let expected = vec![
            Box::new(Int64Array::from(&[Some(2), Some(0), Some(0), Some(-1)])) as Box<dyn Array>,
            Box::new(Utf8Array::<i32>::from(&vec![
                Some("9"),
                Some("foo"),
                Some("bar"),
                Some("7"),
            ])) as Box<dyn Array>,
        ];
        test_lex_sort_arrays(input, expected);
    }

    /*
        // test sort with nulls first
        let input = vec![
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                    None,
                    Some(-1),
                    Some(2),
                    None,
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![
                    Some("foo"),
                    Some("world"),
                    Some("hello"),
                    None,
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
        ];
        let expected = vec![
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                None,
                None,
                Some(2),
                Some(-1),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                None,
                Some("foo"),
                Some("hello"),
                Some("world"),
            ])) as ArrayRef,
        ];
        test_lex_sort_arrays(input, expected);

        // test sort with nulls last
        let input = vec![
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                    None,
                    Some(-1),
                    Some(2),
                    None,
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: false,
                }),
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![
                    Some("foo"),
                    Some("world"),
                    Some("hello"),
                    None,
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: false,
                }),
            },
        ];
        let expected = vec![
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(2),
                Some(-1),
                None,
                None,
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("hello"),
                Some("world"),
                Some("foo"),
                None,
            ])) as ArrayRef,
        ];
        test_lex_sort_arrays(input, expected);

        // test sort with opposite options
        let input = vec![
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                    None,
                    Some(-1),
                    Some(2),
                    Some(-1),
                    None,
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![
                    Some("foo"),
                    Some("bar"),
                    Some("world"),
                    Some("hello"),
                    None,
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
        ];
        let expected = vec![
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(-1),
                Some(-1),
                Some(2),
                None,
                None,
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("hello"),
                Some("bar"),
                Some("world"),
                None,
                Some("foo"),
            ])) as ArrayRef,
        ];
        test_lex_sort_arrays(input, expected);
    }
    */
}
