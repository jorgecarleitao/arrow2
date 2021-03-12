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

//! Defines sort kernel for `ArrayRef`

use std::cmp::{Ordering, Reverse};

use crate::array::ord;
use crate::compute::take;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::{
    array::*,
    buffer::Buffer,
    types::{days_ms, NativeType},
};

use crate::buffer::MutableBuffer;
use num::ToPrimitive;

/// Sort the `ArrayRef` using `SortOptions`.
///
/// Performs a stable sort on values and indices. Nulls are ordered according to the `nulls_first` flag in `options`.
/// Floats are sorted using IEEE 754 totalOrder
///
/// Returns an `ArrowError::ComputeError(String)` if the array type is either unsupported by `sort_to_indices` or `take`.
///
pub fn sort(values: &dyn Array, options: Option<SortOptions>) -> Result<Box<dyn Array>> {
    let indices = sort_to_indices(values, options)?;
    take::take(values, &indices)
}

// partition indices into valid and null indices
fn partition_validity(array: &dyn Array) -> (Vec<i32>, Vec<i32>) {
    let indices = 0..(array.len().to_i32().unwrap());
    indices.partition(|index| array.is_valid(*index as usize))
}

/// Sort elements from `ArrayRef` into an unsigned integer (`UInt32Array`) of indices.
/// For floating point arrays any NaN values are considered to be greater than any other non-null value
pub fn sort_to_indices(values: &dyn Array, options: Option<SortOptions>) -> Result<Int32Array> {
    let options = options.unwrap_or_default();

    let (v, n) = partition_validity(values);

    match values.data_type() {
        DataType::Boolean => sort_boolean(values, v, n, &options),
        DataType::Int8 => sort_primitive::<i8, _>(values, v, n, ord::total_cmp, &options),
        DataType::Int16 => sort_primitive::<i16, _>(values, v, n, ord::total_cmp, &options),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            sort_primitive::<i32, _>(values, v, n, ord::total_cmp, &options)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, None)
        | DataType::Duration(_) => sort_primitive::<i64, _>(values, v, n, ord::total_cmp, &options),
        DataType::UInt8 => sort_primitive::<u8, _>(values, v, n, ord::total_cmp, &options),
        DataType::UInt16 => sort_primitive::<u16, _>(values, v, n, ord::total_cmp, &options),
        DataType::UInt32 => sort_primitive::<u32, _>(values, v, n, ord::total_cmp, &options),
        DataType::UInt64 => sort_primitive::<u64, _>(values, v, n, ord::total_cmp, &options),
        DataType::Float32 => sort_primitive::<f32, _>(values, v, n, ord::total_cmp_f32, &options),
        DataType::Float64 => sort_primitive::<f64, _>(values, v, n, ord::total_cmp_f64, &options),
        DataType::Interval(IntervalUnit::DayTime) => {
            sort_primitive::<days_ms, _>(values, v, n, ord::total_cmp, &options)
        }
        DataType::Utf8 => Ok(sort_utf8::<i32>(values, v, n, &options)),
        DataType::LargeUtf8 => Ok(sort_utf8::<i64>(values, v, n, &options)),
        DataType::List(field) => match field.data_type() {
            DataType::Int8 => Ok(sort_list::<i32, i8>(values, v, n, &options)),
            DataType::Int16 => Ok(sort_list::<i32, i16>(values, v, n, &options)),
            DataType::Int32 => Ok(sort_list::<i32, i32>(values, v, n, &options)),
            DataType::Int64 => Ok(sort_list::<i32, i64>(values, v, n, &options)),
            DataType::UInt8 => Ok(sort_list::<i32, u8>(values, v, n, &options)),
            DataType::UInt16 => Ok(sort_list::<i32, u16>(values, v, n, &options)),
            DataType::UInt32 => Ok(sort_list::<i32, u32>(values, v, n, &options)),
            DataType::UInt64 => Ok(sort_list::<i32, u64>(values, v, n, &options)),
            t => Err(ArrowError::NotYetImplemented(format!(
                "Sort not supported for list type {:?}",
                t
            ))),
        },
        DataType::LargeList(field) => match field.data_type() {
            DataType::Int8 => Ok(sort_list::<i64, i8>(values, v, n, &options)),
            DataType::Int16 => Ok(sort_list::<i64, i16>(values, v, n, &options)),
            DataType::Int32 => Ok(sort_list::<i64, i32>(values, v, n, &options)),
            DataType::Int64 => Ok(sort_list::<i64, i64>(values, v, n, &options)),
            DataType::UInt8 => Ok(sort_list::<i64, u8>(values, v, n, &options)),
            DataType::UInt16 => Ok(sort_list::<i64, u16>(values, v, n, &options)),
            DataType::UInt32 => Ok(sort_list::<i64, u32>(values, v, n, &options)),
            DataType::UInt64 => Ok(sort_list::<i64, u64>(values, v, n, &options)),
            t => Err(ArrowError::NotYetImplemented(format!(
                "Sort not supported for list type {:?}",
                t
            ))),
        },
        DataType::FixedSizeList(field, _) => match field.data_type() {
            DataType::Int8 => Ok(sort_list::<i32, i8>(values, v, n, &options)),
            DataType::Int16 => Ok(sort_list::<i32, i16>(values, v, n, &options)),
            DataType::Int32 => Ok(sort_list::<i32, i32>(values, v, n, &options)),
            DataType::Int64 => Ok(sort_list::<i32, i64>(values, v, n, &options)),
            DataType::UInt8 => Ok(sort_list::<i32, u8>(values, v, n, &options)),
            DataType::UInt16 => Ok(sort_list::<i32, u16>(values, v, n, &options)),
            DataType::UInt32 => Ok(sort_list::<i32, u32>(values, v, n, &options)),
            DataType::UInt64 => Ok(sort_list::<i32, u64>(values, v, n, &options)),
            t => Err(ArrowError::NotYetImplemented(format!(
                "Sort not supported for list type {:?}",
                t
            ))),
        },
        DataType::Dictionary(key_type, value_type) if *value_type.as_ref() == DataType::Utf8 => {
            match key_type.as_ref() {
                DataType::Int8 => Ok(sort_string_dictionary::<i8>(values, v, n, &options)),
                DataType::Int16 => Ok(sort_string_dictionary::<i16>(values, v, n, &options)),
                DataType::Int32 => Ok(sort_string_dictionary::<i32>(values, v, n, &options)),
                DataType::Int64 => Ok(sort_string_dictionary::<i64>(values, v, n, &options)),
                DataType::UInt8 => Ok(sort_string_dictionary::<u8>(values, v, n, &options)),
                DataType::UInt16 => Ok(sort_string_dictionary::<u16>(values, v, n, &options)),
                DataType::UInt32 => Ok(sort_string_dictionary::<u32>(values, v, n, &options)),
                DataType::UInt64 => Ok(sort_string_dictionary::<u64>(values, v, n, &options)),
                t => Err(ArrowError::NotYetImplemented(format!(
                    "Sort not supported for dictionary key type {:?}",
                    t
                ))),
            }
        }
        t => Err(ArrowError::NotYetImplemented(format!(
            "Sort not supported for data type {:?}",
            t
        ))),
    }
}

/// Options that define how sort kernels should behave
#[derive(Clone, Copy, Debug)]
pub struct SortOptions {
    /// Whether to sort in descending order
    pub descending: bool,
    /// Whether to sort nulls first
    pub nulls_first: bool,
}

impl Default for SortOptions {
    fn default() -> Self {
        Self {
            descending: false,
            // default to nulls first to match spark's behavior
            nulls_first: true,
        }
    }
}

/// Sort primitive values
fn sort_boolean(
    values: &dyn Array,
    value_indices: Vec<i32>,
    null_indices: Vec<i32>,
    options: &SortOptions,
) -> Result<Int32Array> {
    let values = values
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("Unable to downcast to boolean array");
    let descending = options.descending;

    // create tuples that are used for sorting
    let mut valids = value_indices
        .into_iter()
        .map(|index| (index, values.value(index as usize)))
        .collect::<Vec<(i32, bool)>>();

    let mut nulls = null_indices;

    if !descending {
        valids.sort_by(|a, b| a.1.cmp(&b.1));
    } else {
        valids.sort_by(|a, b| a.1.cmp(&b.1).reverse());
        // reverse to keep a stable ordering
        nulls.reverse();
    }

    let mut values = MutableBuffer::<i32>::with_capacity(values.len());

    if options.nulls_first {
        values.extend_from_slice(nulls.as_slice());
        valids.iter().for_each(|x| values.push(x.0));
    } else {
        // nulls last
        valids.iter().for_each(|x| values.push(x.0));
        values.extend_from_slice(nulls.as_slice());
    }

    Ok(Int32Array::from_data(DataType::Int32, values.into(), None))
}

/// Sort primitive values
fn sort_primitive<T, F>(
    values: &dyn Array,
    value_indices: Vec<i32>,
    null_indices: Vec<i32>,
    cmp: F,
    options: &SortOptions,
) -> Result<Int32Array>
where
    T: NativeType,
    F: Fn(&T, &T) -> std::cmp::Ordering,
{
    let values = values
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap()
        .values();
    let descending = options.descending;

    // create tuples that are used for sorting
    let mut valids = value_indices
        .into_iter()
        .map(|index| (index, values[index as usize]))
        .collect::<Vec<(i32, T)>>();

    let mut nulls = null_indices;

    if !descending {
        valids.sort_by(|a, b| cmp(&a.1, &b.1));
    } else {
        valids.sort_by(|a, b| cmp(&a.1, &b.1).reverse());
        // reverse to keep a stable ordering
        nulls.reverse();
    }

    let valids = valids.iter().map(|tuple| tuple.0);

    let values = if options.nulls_first {
        let values = nulls.into_iter().chain(valids);
        unsafe { Buffer::<i32>::from_trusted_len_iter(values) }
    } else {
        let values = valids.chain(nulls.into_iter());
        unsafe { Buffer::<i32>::from_trusted_len_iter(values) }
    };

    Ok(Int32Array::from_data(DataType::Int32, values, None))
}

/// Sort strings
fn sort_utf8<O: Offset>(
    values: &dyn Array,
    value_indices: Vec<i32>,
    null_indices: Vec<i32>,
    options: &SortOptions,
) -> Int32Array {
    let values = values.as_any().downcast_ref::<Utf8Array<O>>().unwrap();

    sort_string_helper(
        values,
        value_indices,
        null_indices,
        options,
        |array, idx| array.value(idx as usize),
    )
}

/// Sort dictionary encoded strings
fn sort_string_dictionary<T: DictionaryKey>(
    values: &dyn Array,
    value_indices: Vec<i32>,
    null_indices: Vec<i32>,
    options: &SortOptions,
) -> Int32Array {
    let values: &DictionaryArray<T> = values
        .as_any()
        .downcast_ref::<DictionaryArray<T>>()
        .unwrap();

    let keys = values.keys();

    let dict = values.values();
    let dict = dict.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();

    sort_string_helper(
        keys,
        value_indices,
        null_indices,
        options,
        |array: &PrimitiveArray<T>, idx| -> &str {
            let key: T = array.value(idx as usize);
            dict.value(key.to_usize().unwrap())
        },
    )
}

/// shared implementation between dictionary encoded and plain string arrays
#[inline]
fn sort_string_helper<'a, A: Array, F>(
    values: &'a A,
    value_indices: Vec<i32>,
    null_indices: Vec<i32>,
    options: &SortOptions,
    value_fn: F,
) -> Int32Array
where
    F: Fn(&'a A, i32) -> &str,
{
    let mut valids = value_indices
        .into_iter()
        .map(|index| (index, value_fn(&values, index)))
        .collect::<Vec<(i32, &str)>>();
    let mut nulls = null_indices;
    if !options.descending {
        valids.sort_by_key(|a| a.1);
    } else {
        valids.sort_by_key(|a| Reverse(a.1));
        nulls.reverse();
    }

    let valids = valids.iter().map(|tuple| tuple.0);

    let values = if options.nulls_first {
        let values = nulls.into_iter().chain(valids);
        unsafe { Buffer::<i32>::from_trusted_len_iter(values) }
    } else {
        let values = valids.chain(nulls.into_iter());
        unsafe { Buffer::<i32>::from_trusted_len_iter(values) }
    };

    PrimitiveArray::<i32>::from_data(DataType::Int32, values, None)
}

fn sort_list<O, T>(
    values: &dyn Array,
    value_indices: Vec<i32>,
    null_indices: Vec<i32>,
    options: &SortOptions,
) -> Int32Array
where
    O: Offset,
    T: NativeType + std::cmp::PartialOrd,
{
    let mut valids: Vec<(i32, Box<dyn Array>)> = values
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .map_or_else(
            || {
                let values = values.as_any().downcast_ref::<ListArray<O>>().unwrap();
                value_indices
                    .iter()
                    .copied()
                    .map(|index| (index, values.value(index as usize)))
                    .collect()
            },
            |values| {
                value_indices
                    .iter()
                    .copied()
                    .map(|index| (index, values.value(index as usize)))
                    .collect()
            },
        );

    if !options.descending {
        valids.sort_by(|a, b| cmp_array(a.1.as_ref(), b.1.as_ref()))
    } else {
        valids.sort_by(|a, b| cmp_array(a.1.as_ref(), b.1.as_ref()).reverse())
    }

    let values = valids.iter().map(|tuple| tuple.0);

    let values = if options.nulls_first {
        let mut buffer =
            unsafe { MutableBuffer::<i32>::from_trusted_len_iter(null_indices.into_iter()) };
        values.for_each(|x| buffer.push(x));
        buffer.into()
    } else {
        let mut buffer = unsafe { MutableBuffer::<i32>::from_trusted_len_iter(values) };
        null_indices.iter().for_each(|x| buffer.push(*x));
        buffer.into()
    };

    PrimitiveArray::<i32>::from_data(DataType::Int32, values, None)
}

/// Compare two `Array`s based on the ordering defined in [ord](crate::array::ord).
fn cmp_array(a: &dyn Array, b: &dyn Array) -> Ordering {
    let cmp_op = ord::build_compare(a, b).unwrap();
    let length = a.len().max(b.len());

    for i in 0..length {
        let result = cmp_op(i, i);
        if result != Ordering::Equal {
            return result;
        }
    }
    Ordering::Equal
}

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
pub fn lexsort_to_indices(columns: &[SortColumn]) -> Result<Int32Array> {
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
    use super::*;

    fn test_sort_to_indices_boolean_arrays(
        data: &[Option<bool>],
        options: Option<SortOptions>,
        expected_data: &[i32],
    ) {
        let output = BooleanArray::from(data);
        let expected = Primitive::<i32>::from_slice(&expected_data).to(DataType::Int32);
        let output = sort_to_indices(&output, options).unwrap();
        assert_eq!(output, expected)
    }

    fn test_sort_to_indices_primitive_arrays<T>(
        data: &[Option<T>],
        data_type: DataType,
        options: Option<SortOptions>,
        expected_data: &[i32],
    ) where
        T: NativeType,
    {
        let input = Primitive::<T>::from(data).to(data_type);
        let expected = Primitive::<i32>::from_slice(&expected_data).to(DataType::Int32);
        let output = sort_to_indices(&input, options).unwrap();
        assert_eq!(output, expected)
    }

    fn test_sort_primitive_arrays<T>(
        data: &[Option<T>],
        data_type: DataType,
        options: Option<SortOptions>,
        expected_data: &[Option<T>],
    ) where
        T: NativeType,
    {
        let input = Primitive::<T>::from(data).to(data_type.clone());
        let expected = Primitive::<T>::from(expected_data).to(data_type);
        let output = sort(&input, options).unwrap();
        assert_eq!(expected, output.as_ref())
    }

    fn test_sort_to_indices_string_arrays(
        data: &[Option<&str>],
        options: Option<SortOptions>,
        expected_data: &[i32],
    ) {
        let input = Utf8Array::<i32>::from(&data.to_vec());
        let expected = Primitive::<i32>::from_slice(&expected_data).to(DataType::Int32);
        let output = sort_to_indices(&input, options).unwrap();
        assert_eq!(output, expected)
    }

    fn test_sort_string_arrays(
        data: &[Option<&str>],
        options: Option<SortOptions>,
        expected_data: &[Option<&str>],
    ) {
        let input = Utf8Array::<i32>::from(&data.to_vec());
        let expected = Utf8Array::<i32>::from(&expected_data.to_vec());
        let output = sort(&input, options).unwrap();
        assert_eq!(expected, output.as_ref())
    }

    fn test_sort_string_dict_arrays<K: DictionaryKey>(
        data: &[Option<&str>],
        options: Option<SortOptions>,
        expected_data: &[Option<&str>],
    ) {
        let input = data.iter().map(|x| Result::Ok(x.clone()));
        let input =
            DictionaryPrimitive::<i32, Utf8Primitive<i32>, &str>::try_from_iter(input).unwrap();
        let input = input.to(DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        ));

        let expected = expected_data.iter().map(|x| Result::Ok(x.clone()));
        let expected =
            DictionaryPrimitive::<i32, Utf8Primitive<i32>, &str>::try_from_iter(expected).unwrap();
        let expected = expected.to(DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        ));

        let output = sort(&input, options).unwrap();
        assert_eq!(expected, output.as_ref())
    }

    /*
    fn test_sort_list_arrays<T>(
        data: Vec<Option<Vec<Option<T::Native>>>>,
        options: Option<SortOptions>,
        expected_data: Vec<Option<Vec<Option<T::Native>>>>,
        fixed_length: Option<i32>,
    ) where
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    {
        // for FixedSizedList
        if let Some(length) = fixed_length {
            let input = Arc::new(build_fixed_size_list_nullable(data.clone(), length));
            let sorted = sort(&(input as ArrayRef), options).unwrap();
            let expected = Arc::new(build_fixed_size_list_nullable(
                expected_data.clone(),
                length,
            )) as ArrayRef;

            assert_eq!(&sorted, &expected);
        }

        // for List
        let input = Arc::new(build_generic_list_nullable::<i32, T>(data.clone()));
        let sorted = sort(&(input as ArrayRef), options).unwrap();
        let expected =
            Arc::new(build_generic_list_nullable::<i32, T>(expected_data.clone()))
                as ArrayRef;

        assert_eq!(&sorted, &expected);

        // for LargeList
        let input = Arc::new(build_generic_list_nullable::<i64, T>(data));
        let sorted = sort(&(input as ArrayRef), options).unwrap();
        let expected =
            Arc::new(build_generic_list_nullable::<i64, T>(expected_data)) as ArrayRef;

        assert_eq!(&sorted, &expected);
    }

    fn test_lex_sort_arrays(input: Vec<SortColumn>, expected_output: Vec<ArrayRef>) {
        let sorted = lexsort(&input).unwrap();

        for (result, expected) in sorted.iter().zip(expected_output.iter()) {
            assert_eq!(result, expected);
        }
    }
    */

    #[test]
    fn test_sort_to_indices_primitives() {
        test_sort_to_indices_primitive_arrays::<i16>(
            &[None, Some(0), Some(2), Some(-1), Some(0), None],
            DataType::Int16,
            None,
            &[0, 5, 3, 1, 4, 2],
        );
        test_sort_to_indices_primitive_arrays::<f32>(
            &[
                None,
                Some(-0.05),
                Some(2.225),
                Some(-1.01),
                Some(-0.05),
                None,
            ],
            DataType::Float32,
            None,
            &[0, 5, 3, 1, 4, 2],
        );
        test_sort_to_indices_primitive_arrays::<f64>(
            &[
                None,
                Some(-0.05),
                Some(2.225),
                Some(-1.01),
                Some(-0.05),
                None,
            ],
            DataType::Float64,
            None,
            &[0, 5, 3, 1, 4, 2],
        );

        // descending
        test_sort_to_indices_primitive_arrays::<i8>(
            &[None, Some(0), Some(2), Some(-1), Some(0), None],
            DataType::Int8,
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            &[2, 1, 4, 3, 5, 0], // [2, 4, 1, 3, 5, 0]
        );

        test_sort_to_indices_primitive_arrays::<f32>(
            &[
                None,
                Some(0.005),
                Some(20.22),
                Some(-10.3),
                Some(0.005),
                None,
            ],
            DataType::Float32,
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            &[2, 1, 4, 3, 5, 0],
        );

        test_sort_to_indices_primitive_arrays::<f64>(
            &[None, Some(0.0), Some(2.0), Some(-1.0), Some(0.0), None],
            DataType::Float64,
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            &[2, 1, 4, 3, 5, 0],
        );

        // descending, nulls first
        test_sort_to_indices_primitive_arrays::<i8>(
            &[None, Some(0), Some(2), Some(-1), Some(0), None],
            DataType::Int8,
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            &[5, 0, 2, 1, 4, 3], // [5, 0, 2, 4, 1, 3]
        );

        test_sort_to_indices_primitive_arrays::<f32>(
            &[None, Some(0.1), Some(0.2), Some(-1.3), Some(0.01), None],
            DataType::Float32,
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            &[5, 0, 2, 1, 4, 3],
        );

        test_sort_to_indices_primitive_arrays::<f64>(
            &[None, Some(10.1), Some(100.2), Some(-1.3), Some(10.01), None],
            DataType::Float64,
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            &[5, 0, 2, 1, 4, 3],
        );
    }

    #[test]
    fn test_sort_boolean() {
        // boolean
        test_sort_to_indices_boolean_arrays(
            &[None, Some(false), Some(true), Some(true), Some(false), None],
            None,
            &[0, 5, 1, 4, 2, 3],
        );

        // boolean, descending
        test_sort_to_indices_boolean_arrays(
            &[None, Some(false), Some(true), Some(true), Some(false), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            &[2, 3, 1, 4, 5, 0],
        );

        // boolean, descending, nulls first
        test_sort_to_indices_boolean_arrays(
            &[None, Some(false), Some(true), Some(true), Some(false), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            &[5, 0, 2, 3, 1, 4],
        );
    }

    #[test]
    fn test_sort_primitives() {
        // default case
        test_sort_primitive_arrays::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            None,
            &[None, None, Some(2), Some(3), Some(3), Some(5)],
        );
        // descending
        test_sort_primitive_arrays::<i8>(
            &[None, Some(0), Some(2), Some(-1), Some(0), None],
            DataType::Int8,
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            &[Some(2), Some(0), Some(0), Some(-1), None, None],
        );

        // descending, nulls first
        test_sort_primitive_arrays::<i8>(
            &[None, Some(0), Some(2), Some(-1), Some(0), None],
            DataType::Int8,
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            &[None, None, Some(2), Some(0), Some(0), Some(-1)],
        );

        test_sort_primitive_arrays::<f32>(
            &[None, Some(0.0), Some(2.0), Some(-1.0), Some(0.0), None],
            DataType::Float32,
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            &[None, None, Some(2.0), Some(0.0), Some(0.0), Some(-1.0)],
        );

        // ascending, nulls first
        test_sort_primitive_arrays::<i8>(
            &[None, Some(0), Some(2), Some(-1), Some(0), None],
            DataType::Int8,
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            &[None, None, Some(-1), Some(0), Some(0), Some(2)],
        );
        test_sort_primitive_arrays::<f32>(
            &[None, Some(0.0), Some(2.0), Some(-1.0), Some(0.0), None],
            DataType::Float32,
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            &[None, None, Some(-1.0), Some(0.0), Some(0.0), Some(2.0)],
        );
    }

    #[test]
    #[ignore] // improve equality for NaN values. These are right but the equality fails
    fn test_nans() {
        test_sort_primitive_arrays::<f64>(
            &[None, Some(0.0), Some(2.0), Some(-1.0), Some(f64::NAN), None],
            DataType::Float64,
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            &[None, None, Some(f64::NAN), Some(2.0), Some(0.0), Some(-1.0)],
        );
        test_sort_primitive_arrays::<f64>(
            &[Some(f64::NAN), Some(f64::NAN), Some(f64::NAN), Some(1.0)],
            DataType::Float64,
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            &[Some(f64::NAN), Some(f64::NAN), Some(f64::NAN), Some(1.0)],
        );

        test_sort_primitive_arrays::<f64>(
            &[None, Some(0.0), Some(2.0), Some(-1.0), Some(f64::NAN), None],
            DataType::Float64,
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            &[None, None, Some(-1.0), Some(0.0), Some(2.0), Some(f64::NAN)],
        );
        // nans
        test_sort_primitive_arrays::<f64>(
            &[Some(f64::NAN), Some(f64::NAN), Some(f64::NAN), Some(1.0)],
            DataType::Float64,
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            &[Some(1.0), Some(f64::NAN), Some(f64::NAN), Some(f64::NAN)],
        );
    }

    #[test]
    fn test_sort_to_indices_strings() {
        test_sort_to_indices_string_arrays(
            &[
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            None,
            &[0, 3, 5, 1, 4, 2],
        );

        test_sort_to_indices_string_arrays(
            &[
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            &[2, 4, 1, 5, 3, 0],
        );

        test_sort_to_indices_string_arrays(
            &[
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            &[0, 3, 5, 1, 4, 2],
        );

        test_sort_to_indices_string_arrays(
            &[
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            &[3, 0, 2, 4, 1, 5],
        );
    }

    #[test]
    fn test_sort_strings() {
        test_sort_string_arrays(
            &[
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            None,
            &[
                None,
                None,
                Some("-ad"),
                Some("bad"),
                Some("glad"),
                Some("sad"),
            ],
        );

        test_sort_string_arrays(
            &[
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            &[
                Some("sad"),
                Some("glad"),
                Some("bad"),
                Some("-ad"),
                None,
                None,
            ],
        );

        test_sort_string_arrays(
            &[
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            &[
                None,
                None,
                Some("-ad"),
                Some("bad"),
                Some("glad"),
                Some("sad"),
            ],
        );

        test_sort_string_arrays(
            &[
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            &[
                None,
                None,
                Some("sad"),
                Some("glad"),
                Some("bad"),
                Some("-ad"),
            ],
        );
    }

    #[test]
    fn test_sort_string_dicts() {
        test_sort_string_dict_arrays::<i8>(
            &[
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            None,
            &[
                None,
                None,
                Some("-ad"),
                Some("bad"),
                Some("glad"),
                Some("sad"),
            ],
        );

        test_sort_string_dict_arrays::<i16>(
            &[
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            &[
                Some("sad"),
                Some("glad"),
                Some("bad"),
                Some("-ad"),
                None,
                None,
            ],
        );

        test_sort_string_dict_arrays::<i32>(
            &[
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            &[
                None,
                None,
                Some("-ad"),
                Some("bad"),
                Some("glad"),
                Some("sad"),
            ],
        );

        test_sort_string_dict_arrays::<i16>(
            &[
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            &[
                None,
                None,
                Some("sad"),
                Some("glad"),
                Some("bad"),
                Some("-ad"),
            ],
        );
    }

    /*
    #[test]
    fn test_sort_list() {
        test_sort_list_arrays::<i8>(
            vec![
                Some(vec![Some(1)]),
                Some(vec![Some(4)]),
                Some(vec![Some(2)]),
                Some(vec![Some(3)]),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            vec![
                Some(vec![Some(1)]),
                Some(vec![Some(2)]),
                Some(vec![Some(3)]),
                Some(vec![Some(4)]),
            ],
            Some(1),
        );

        test_sort_list_arrays::<i32>(
            vec![
                Some(vec![Some(1), Some(0)]),
                Some(vec![Some(4), Some(3), Some(2), Some(1)]),
                Some(vec![Some(2), Some(3), Some(4)]),
                Some(vec![Some(3), Some(3), Some(3), Some(3)]),
                Some(vec![Some(1), Some(1)]),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            vec![
                Some(vec![Some(1), Some(0)]),
                Some(vec![Some(1), Some(1)]),
                Some(vec![Some(2), Some(3), Some(4)]),
                Some(vec![Some(3), Some(3), Some(3), Some(3)]),
                Some(vec![Some(4), Some(3), Some(2), Some(1)]),
            ],
            None,
        );

        test_sort_list_arrays::<i32>(
            vec![
                None,
                Some(vec![Some(4), None, Some(2)]),
                Some(vec![Some(2), Some(3), Some(4)]),
                None,
                Some(vec![Some(3), Some(3), None]),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            vec![
                Some(vec![Some(2), Some(3), Some(4)]),
                Some(vec![Some(3), Some(3), None]),
                Some(vec![Some(4), None, Some(2)]),
                None,
                None,
            ],
            Some(3),
        );
    }

    #[test]
    fn test_lex_sort_single_column() {
        let input = vec![SortColumn {
            values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(17),
                Some(2),
                Some(-1),
                Some(0),
            ])) as ArrayRef,
            options: None,
        }];
        let expected = vec![Arc::new(PrimitiveArray::<Int64Type>::from(vec![
            Some(-1),
            Some(0),
            Some(2),
            Some(17),
        ])) as ArrayRef];
        test_lex_sort_arrays(input, expected);
    }

    #[test]
    fn test_lex_sort_unaligned_rows() {
        let input = vec![
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![None, Some(-1)]))
                    as ArrayRef,
                options: None,
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![Some("foo")])) as ArrayRef,
                options: None,
            },
        ];
        assert!(
            lexsort(&input).is_err(),
            "lexsort should reject columns with different row counts"
        );
    }

    #[test]
    fn test_lex_sort_mixed_types() {
        let input = vec![
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                    Some(0),
                    Some(2),
                    Some(-1),
                    Some(0),
                ])) as ArrayRef,
                options: None,
            },
            SortColumn {
                values: Arc::new(PrimitiveArray::<UInt32Type>::from(vec![
                    Some(101),
                    Some(8),
                    Some(7),
                    Some(102),
                ])) as ArrayRef,
                options: None,
            },
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                    Some(-1),
                    Some(-2),
                    Some(-3),
                    Some(-4),
                ])) as ArrayRef,
                options: None,
            },
        ];
        let expected = vec![
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(-1),
                Some(0),
                Some(0),
                Some(2),
            ])) as ArrayRef,
            Arc::new(PrimitiveArray::<UInt32Type>::from(vec![
                Some(7),
                Some(101),
                Some(102),
                Some(8),
            ])) as ArrayRef,
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(-3),
                Some(-1),
                Some(-4),
                Some(-2),
            ])) as ArrayRef,
        ];
        test_lex_sort_arrays(input, expected);

        // test mix of string and in64 with option
        let input = vec![
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                    Some(0),
                    Some(2),
                    Some(-1),
                    Some(0),
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![
                    Some("foo"),
                    Some("9"),
                    Some("7"),
                    Some("bar"),
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
        ];
        let expected = vec![
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(2),
                Some(0),
                Some(0),
                Some(-1),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("9"),
                Some("foo"),
                Some("bar"),
                Some("7"),
            ])) as ArrayRef,
        ];
        test_lex_sort_arrays(input, expected);

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
