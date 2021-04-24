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

mod lex_sort;
mod primitive;

pub use lex_sort::{lexsort, lexsort_to_indices, SortColumn};

macro_rules! dyn_sort {
    ($ty:ty, $array:expr, $cmp:expr, $options:expr) => {{
        let array = $array
            .as_any()
            .downcast_ref::<PrimitiveArray<$ty>>()
            .unwrap();
        Ok(Box::new(primitive::sort_by::<$ty, _>(
            &array, $cmp, $options,
        )))
    }};
}

/// Sort the `ArrayRef` using `SortOptions`.
///
/// Performs a stable sort on values and indices. Nulls are ordered according to the `nulls_first` flag in `options`.
/// Floats are sorted using IEEE 754 totalOrder
///
/// Returns an `ArrowError::ComputeError(String)` if the array type is either unsupported by `sort_to_indices` or `take`.
///
pub fn sort(values: &dyn Array, options: &SortOptions) -> Result<Box<dyn Array>> {
    match values.data_type() {
        DataType::Int8 => dyn_sort!(i8, values, ord::total_cmp, options),
        DataType::Int16 => dyn_sort!(i16, values, ord::total_cmp, options),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            dyn_sort!(i32, values, ord::total_cmp, options)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, None)
        | DataType::Duration(_) => dyn_sort!(i64, values, ord::total_cmp, options),
        DataType::UInt8 => dyn_sort!(u8, values, ord::total_cmp, options),
        DataType::UInt16 => dyn_sort!(u16, values, ord::total_cmp, options),
        DataType::UInt32 => dyn_sort!(u32, values, ord::total_cmp, options),
        DataType::UInt64 => dyn_sort!(u64, values, ord::total_cmp, options),
        DataType::Float32 => dyn_sort!(f32, values, ord::total_cmp_f32, options),
        DataType::Float64 => dyn_sort!(f64, values, ord::total_cmp_f64, options),
        DataType::Interval(IntervalUnit::DayTime) => {
            dyn_sort!(days_ms, values, ord::total_cmp, options)
        }
        _ => {
            let indices = sort_to_indices(values, options)?;
            take::take(values, &indices)
        }
    }
}

// partition indices into valid and null indices
fn partition_validity(array: &dyn Array) -> (Vec<i32>, Vec<i32>) {
    let indices = 0..(array.len().to_i32().unwrap());
    indices.partition(|index| array.is_valid(*index as usize))
}

macro_rules! dyn_sort_indices {
    ($ty:ty, $array:expr, $cmp:expr, $options:expr) => {{
        let array = $array
            .as_any()
            .downcast_ref::<PrimitiveArray<$ty>>()
            .unwrap();
        Ok(primitive::indices_sorted_by::<$ty, _>(
            &array, $cmp, $options,
        ))
    }};
}

/// Sort elements from `ArrayRef` into an unsigned integer (`UInt32Array`) of indices.
/// For floating point arrays any NaN values are considered to be greater than any other non-null value
pub fn sort_to_indices(values: &dyn Array, options: &SortOptions) -> Result<Int32Array> {
    match values.data_type() {
        DataType::Boolean => {
            let (v, n) = partition_validity(values);
            Ok(sort_boolean(values, v, n, &options))
        }
        DataType::Int8 => dyn_sort_indices!(i8, values, ord::total_cmp, options),
        DataType::Int16 => dyn_sort_indices!(i16, values, ord::total_cmp, options),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            dyn_sort_indices!(i32, values, ord::total_cmp, options)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, None)
        | DataType::Duration(_) => dyn_sort_indices!(i64, values, ord::total_cmp, options),
        DataType::UInt8 => dyn_sort_indices!(u8, values, ord::total_cmp, options),
        DataType::UInt16 => dyn_sort_indices!(u16, values, ord::total_cmp, options),
        DataType::UInt32 => dyn_sort_indices!(u32, values, ord::total_cmp, options),
        DataType::UInt64 => dyn_sort_indices!(u64, values, ord::total_cmp, options),
        DataType::Float32 => dyn_sort_indices!(f32, values, ord::total_cmp_f32, options),
        DataType::Float64 => dyn_sort_indices!(f64, values, ord::total_cmp_f64, options),
        DataType::Interval(IntervalUnit::DayTime) => {
            dyn_sort_indices!(days_ms, values, ord::total_cmp, options)
        }
        DataType::Utf8 => {
            let (v, n) = partition_validity(values);
            Ok(sort_utf8::<i32>(values, v, n, &options))
        }
        DataType::LargeUtf8 => {
            let (v, n) = partition_validity(values);
            Ok(sort_utf8::<i64>(values, v, n, &options))
        }
        DataType::List(field) => {
            let (v, n) = partition_validity(values);
            match field.data_type() {
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
            }
        }
        DataType::LargeList(field) => {
            let (v, n) = partition_validity(values);
            match field.data_type() {
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
            }
        }
        DataType::FixedSizeList(field, _) => {
            let (v, n) = partition_validity(values);
            match field.data_type() {
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
            }
        }
        DataType::Dictionary(key_type, value_type) if *value_type.as_ref() == DataType::Utf8 => {
            let (v, n) = partition_validity(values);
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
) -> Int32Array {
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

    Int32Array::from_data(DataType::Int32, values.into(), None)
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
        Buffer::<i32>::from_trusted_len_iter(values)
    } else {
        let values = valids.chain(nulls.into_iter());
        Buffer::<i32>::from_trusted_len_iter(values)
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
        let mut buffer = MutableBuffer::<i32>::from_trusted_len_iter(null_indices.into_iter());
        values.for_each(|x| buffer.push(x));
        buffer.into()
    } else {
        let mut buffer = MutableBuffer::<i32>::from_trusted_len_iter(values);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_sort_to_indices_boolean_arrays(
        data: &[Option<bool>],
        options: SortOptions,
        expected_data: &[i32],
    ) {
        let output = BooleanArray::from(data);
        let expected = Primitive::<i32>::from_slice(&expected_data).to(DataType::Int32);
        let output = sort_to_indices(&output, &options).unwrap();
        assert_eq!(output, expected)
    }

    fn test_sort_primitive_arrays<T>(
        data: &[Option<T>],
        data_type: DataType,
        options: SortOptions,
        expected_data: &[Option<T>],
    ) where
        T: NativeType,
    {
        let input = Primitive::<T>::from(data).to(data_type.clone());
        let expected = Primitive::<T>::from(expected_data).to(data_type);
        let output = sort(&input, &options).unwrap();
        assert_eq!(expected, output.as_ref())
    }

    fn test_sort_to_indices_string_arrays(
        data: &[Option<&str>],
        options: SortOptions,
        expected_data: &[i32],
    ) {
        let input = Utf8Array::<i32>::from(&data.to_vec());
        let expected = Primitive::<i32>::from_slice(&expected_data).to(DataType::Int32);
        let output = sort_to_indices(&input, &options).unwrap();
        assert_eq!(output, expected)
    }

    fn test_sort_string_arrays(
        data: &[Option<&str>],
        options: SortOptions,
        expected_data: &[Option<&str>],
    ) {
        let input = Utf8Array::<i32>::from(&data.to_vec());
        let expected = Utf8Array::<i32>::from(&expected_data.to_vec());
        let output = sort(&input, &options).unwrap();
        assert_eq!(expected, output.as_ref())
    }

    fn test_sort_string_dict_arrays<K: DictionaryKey>(
        data: &[Option<&str>],
        options: SortOptions,
        expected_data: &[Option<&str>],
    ) {
        let input = data.iter().map(|x| Result::Ok(*x));
        let input =
            DictionaryPrimitive::<i32, Utf8Primitive<i32>, &str>::try_from_iter(input).unwrap();
        let input = input.to(DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        ));

        let expected = expected_data.iter().map(|x| Result::Ok(*x));
        let expected =
            DictionaryPrimitive::<i32, Utf8Primitive<i32>, &str>::try_from_iter(expected).unwrap();
        let expected = expected.to(DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        ));

        let output = sort(&input, &options).unwrap();
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
    fn test_sort_boolean() {
        // boolean
        test_sort_to_indices_boolean_arrays(
            &[None, Some(false), Some(true), Some(true), Some(false), None],
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            &[0, 5, 1, 4, 2, 3],
        );

        // boolean, descending
        test_sort_to_indices_boolean_arrays(
            &[None, Some(false), Some(true), Some(true), Some(false), None],
            SortOptions {
                descending: true,
                nulls_first: false,
            },
            &[2, 3, 1, 4, 5, 0],
        );

        // boolean, descending, nulls first
        test_sort_to_indices_boolean_arrays(
            &[None, Some(false), Some(true), Some(true), Some(false), None],
            SortOptions {
                descending: true,
                nulls_first: true,
            },
            &[5, 0, 2, 3, 1, 4],
        );
    }

    #[test]
    #[ignore] // improve equality for NaN values. These are right but the equality fails
    fn test_nans() {
        test_sort_primitive_arrays::<f64>(
            &[None, Some(0.0), Some(2.0), Some(-1.0), Some(f64::NAN), None],
            DataType::Float64,
            SortOptions {
                descending: true,
                nulls_first: true,
            },
            &[None, None, Some(f64::NAN), Some(2.0), Some(0.0), Some(-1.0)],
        );
        test_sort_primitive_arrays::<f64>(
            &[Some(f64::NAN), Some(f64::NAN), Some(f64::NAN), Some(1.0)],
            DataType::Float64,
            SortOptions {
                descending: true,
                nulls_first: true,
            },
            &[Some(f64::NAN), Some(f64::NAN), Some(f64::NAN), Some(1.0)],
        );

        test_sort_primitive_arrays::<f64>(
            &[None, Some(0.0), Some(2.0), Some(-1.0), Some(f64::NAN), None],
            DataType::Float64,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            &[None, None, Some(-1.0), Some(0.0), Some(2.0), Some(f64::NAN)],
        );
        // nans
        test_sort_primitive_arrays::<f64>(
            &[Some(f64::NAN), Some(f64::NAN), Some(f64::NAN), Some(1.0)],
            DataType::Float64,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
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
            SortOptions {
                descending: false,
                nulls_first: true,
            },
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
            SortOptions {
                descending: true,
                nulls_first: false,
            },
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
            SortOptions {
                descending: false,
                nulls_first: true,
            },
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
            SortOptions {
                descending: true,
                nulls_first: true,
            },
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
            SortOptions {
                descending: false,
                nulls_first: true,
            },
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
            SortOptions {
                descending: true,
                nulls_first: false,
            },
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
            SortOptions {
                descending: false,
                nulls_first: true,
            },
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
            SortOptions {
                descending: true,
                nulls_first: true,
            },
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
            SortOptions {
                descending: false,
                nulls_first: true,
            },
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
            SortOptions {
                descending: true,
                nulls_first: false,
            },
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
            SortOptions {
                descending: false,
                nulls_first: true,
            },
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
            SortOptions {
                descending: true,
                nulls_first: true,
            },
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
    */
}
