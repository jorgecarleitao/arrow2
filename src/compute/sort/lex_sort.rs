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
    buffer::Buffer,
    datatypes::DataType,
};

use super::{sort_to_indices, SortOptions};

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
/// use arrow2::array::{Utf8Array, PrimitiveArray, Array, Primitive};
/// use arrow2::compute::sort::{SortColumn, SortOptions, lexsort};
/// use arrow2::datatypes::DataType;
///
/// let int64 = Primitive::<i64>::from(&[None, Some(-2), Some(89), Some(-64), Some(101)]).to(DataType::Int64);
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
/// let sorted = sorted_columns[0].as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
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
        return sort_to_indices(column.values, column.options);
    }

    let row_count = columns[0].values.len();
    if columns.iter().any(|item| item.values.len() != row_count) {
        return Err(ArrowError::InvalidArgumentError(
            "lexical sort columns have different row counts".to_string(),
        ));
    };

    // map to data and DynComparator
    let flat_columns = columns
        .iter()
        .map(
            |column| -> Result<(&dyn Array, ord::DynComparator, SortOptions)> {
                // flatten and convert build comparators
                let array = column.values;
                Ok((
                    array,
                    ord::build_compare(array, array)?,
                    column.options.unwrap_or_default(),
                ))
            },
        )
        .collect::<Result<Vec<(&dyn Array, ord::DynComparator, SortOptions)>>>()?;

    let lex_comparator = |a_idx: &usize, b_idx: &usize| -> Ordering {
        for (array, comparator, sort_option) in flat_columns.iter() {
            match (array.is_valid(*a_idx), array.is_valid(*b_idx)) {
                (true, true) => {
                    match (comparator)(*a_idx, *b_idx) {
                        // equal, move on to next column
                        Ordering::Equal => continue,
                        order => {
                            if sort_option.descending {
                                return order.reverse();
                            } else {
                                return order;
                            }
                        }
                    }
                }
                (false, true) => {
                    return if sort_option.nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    };
                }
                (true, false) => {
                    return if sort_option.nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    };
                }
                // equal, move on to next column
                (false, false) => continue,
            }
        }

        Ordering::Equal
    };

    let mut value_indices = (0..row_count).collect::<Vec<usize>>();
    value_indices.sort_by(lex_comparator);

    let values = value_indices.into_iter().map(|i| i as i32);
    let values = unsafe { Buffer::<i32>::from_trusted_len_iter(values) };
    Ok(PrimitiveArray::<i32>::from_data(
        DataType::Int32,
        values,
        None,
    ))
}

#[cfg(test)]
mod tests {
    use crate::array::{Primitive, Utf8Array};

    use super::*;

    fn test_lex_sort_arrays(input: Vec<SortColumn>, expected: Vec<Box<dyn Array>>) {
        let sorted = lexsort(&input).unwrap();
        assert_eq!(sorted, expected);
    }

    #[test]
    fn test_lex_sort_mixed_types() {
        let c1 = Primitive::<i64>::from(&[Some(0), Some(2), Some(-1), Some(0)]).to(DataType::Int64);
        let c2 =
            Primitive::<u32>::from(&[Some(101), Some(8), Some(7), Some(102)]).to(DataType::UInt32);
        let c3 =
            Primitive::<i64>::from(&[Some(-1), Some(-2), Some(-3), Some(-4)]).to(DataType::Int64);

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
        let c1 = Primitive::<i64>::from(&[Some(-1), Some(0), Some(0), Some(2)]).to(DataType::Int64);
        let c2 =
            Primitive::<u32>::from(&[Some(7), Some(101), Some(102), Some(8)]).to(DataType::UInt32);
        let c3 =
            Primitive::<i64>::from(&[Some(-3), Some(-1), Some(-4), Some(-2)]).to(DataType::Int64);
        let expected = vec![Box::new(c1) as Box<dyn Array>, Box::new(c2), Box::new(c3)];
        test_lex_sort_arrays(input, expected);
    }

    #[test]
    fn test_lex_sort_mixed_types2() {
        // test mix of string and in64 with option
        let c1 = Primitive::<i64>::from(&[Some(0), Some(2), Some(-1), Some(0)])
        .to(DataType::Int64);
        let c2 = Utf8Array::<i32>::from(&vec![
            Some("foo"),
            Some("9"),
            Some("7"),
            Some("bar"),
        ]);
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
            Box::new(Primitive::<i64>::from(&[Some(2), Some(0), Some(0), Some(-1)]).to(DataType::Int64)) as Box<dyn Array>,
            Box::new(Utf8Array::<i32>::from(&vec![Some("9"), Some("foo"), Some("bar"), Some("7")])) as Box<dyn Array>,
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
