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

//! Defines kernel to extract a substring of a \[Large\]StringArray

use crate::{array::*, buffer::MutableBuffer};
use crate::{
    datatypes::DataType,
    error::{ArrowError, Result},
};

fn utf8_substring<O: Offset>(array: &Utf8Array<O>, start: O, length: &Option<O>) -> Utf8Array<O> {
    let validity = array.validity();
    let offsets = array.offsets();
    let values = array.values();

    let mut new_offsets = MutableBuffer::<O>::with_capacity(array.len() + 1);
    let mut new_values = MutableBuffer::<u8>::new(); // we have no way to estimate how much this will be.

    let mut length_so_far = O::zero();
    new_offsets.push(length_so_far);

    offsets.windows(2).for_each(|windows| {
        let length_i: O = windows[1] - windows[0];

        // compute where we should start slicing this entry
        let start = windows[0]
            + if start >= O::zero() {
                start
            } else {
                length_i + start
            };
        let start = start.max(windows[0]).min(windows[1]);

        let length: O = length
            .unwrap_or(length_i)
            // .max(0) is not needed as it is guaranteed
            .min(windows[1] - start); // so we do not go beyond this entry
        length_so_far += length;
        new_offsets.push(length_so_far);

        // we need usize for ranges
        let start = start.to_usize();
        let length = length.to_usize();

        new_values.extend_from_slice(&values[start..start + length]);
    });

    Utf8Array::<O>::from_data(
        array.data_type().clone(),
        new_offsets.into(),
        new_values.into(),
        validity.clone(),
    )
}

fn binary_substring<O: Offset>(
    array: &BinaryArray<O>,
    start: O,
    length: &Option<O>,
) -> BinaryArray<O> {
    let validity = array.validity();
    let offsets = array.offsets();
    let values = array.values();

    let mut new_offsets = MutableBuffer::<O>::with_capacity(array.len() + 1);
    let mut new_values = MutableBuffer::<u8>::new(); // we have no way to estimate how much this will be.

    let mut length_so_far = O::zero();
    new_offsets.push(length_so_far);

    offsets.windows(2).for_each(|windows| {
        let length_i: O = windows[1] - windows[0];

        // compute where we should start slicing this entry
        let start = windows[0]
            + if start >= O::zero() {
                start
            } else {
                length_i + start
            };
        let start = start.max(windows[0]).min(windows[1]);

        let length: O = length
            .unwrap_or(length_i)
            // .max(0) is not needed as it is guaranteed
            .min(windows[1] - start); // so we do not go beyond this entry
        length_so_far += length;
        new_offsets.push(length_so_far);

        // we need usize for ranges
        let start = start.to_usize();
        let length = length.to_usize();

        new_values.extend_from_slice(&values[start..start + length]);
    });

    BinaryArray::<O>::from_data(
        array.data_type().clone(),
        new_offsets.into(),
        new_values.into(),
        validity.clone(),
    )
}

/// Returns an ArrayRef with a substring starting from `start` and with optional length `length` of each of the elements in `array`.
/// `start` can be negative, in which case the start counts from the end of the string.
/// this function errors when the passed array is not a \[Large\]String array.
pub fn substring(array: &dyn Array, start: i64, length: &Option<u64>) -> Result<Box<dyn Array>> {
    match array.data_type() {
        DataType::Binary => Ok(Box::new(binary_substring(
            array
                .as_any()
                .downcast_ref::<BinaryArray<i32>>()
                .expect("A binary is expected"),
            start as i32,
            &length.map(|e| e as i32),
        ))),
        DataType::LargeBinary => Ok(Box::new(binary_substring(
            array
                .as_any()
                .downcast_ref::<BinaryArray<i64>>()
                .expect("A large binary is expected"),
            start,
            &length.map(|e| e as i64),
        ))),
        DataType::LargeUtf8 => Ok(Box::new(utf8_substring(
            array
                .as_any()
                .downcast_ref::<Utf8Array<i64>>()
                .expect("A large string is expected"),
            start,
            &length.map(|e| e as i64),
        ))),
        DataType::Utf8 => Ok(Box::new(utf8_substring(
            array
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .expect("A string is expected"),
            start as i32,
            &length.map(|e| e as i32),
        ))),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "substring does not support type {:?}",
            array.data_type()
        ))),
    }
}

/// Checks if an array of type `datatype` can perform substring operation
///
/// # Examples
/// ```
/// use arrow2::compute::substring::can_substring;
/// use arrow2::datatypes::{DataType};
///
/// let data_type = DataType::Utf8;
/// assert_eq!(can_substring(&data_type), true);

/// let data_type = DataType::Null;
/// assert_eq!(can_substring(&data_type), false);
/// ```
pub fn can_substring(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::LargeUtf8 | DataType::Utf8 | DataType::LargeBinary | DataType::Binary
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_nulls_utf8<O: Offset>() -> Result<()> {
        let cases = vec![
            // identity
            (
                vec![Some("hello"), None, Some("word")],
                0,
                None,
                vec![Some("hello"), None, Some("word")],
            ),
            // 0 length -> Nothing
            (
                vec![Some("hello"), None, Some("word")],
                0,
                Some(0),
                vec![Some(""), None, Some("")],
            ),
            // high start -> Nothing
            (
                vec![Some("hello"), None, Some("word")],
                1000,
                Some(0),
                vec![Some(""), None, Some("")],
            ),
            // high negative start -> identity
            (
                vec![Some("hello"), None, Some("word")],
                -1000,
                None,
                vec![Some("hello"), None, Some("word")],
            ),
            // high length -> identity
            (
                vec![Some("hello"), None, Some("word")],
                0,
                Some(1000),
                vec![Some("hello"), None, Some("word")],
            ),
        ];

        cases
            .into_iter()
            .try_for_each::<_, Result<()>>(|(array, start, length, expected)| {
                let array = Utf8Array::<O>::from(&array);
                let result = substring(&array, start, &length)?;
                assert_eq!(array.len(), result.len());

                let result = result.as_any().downcast_ref::<Utf8Array<O>>().unwrap();
                let expected = Utf8Array::<O>::from(&expected);
                assert_eq!(&expected, result);
                Ok(())
            })?;

        Ok(())
    }

    #[test]
    fn with_nulls_string() -> Result<()> {
        with_nulls_utf8::<i32>()
    }

    #[test]
    fn with_nulls_large_string() -> Result<()> {
        with_nulls_utf8::<i64>()
    }

    fn without_nulls_utf8<O: Offset>() -> Result<()> {
        let cases = vec![
            // increase start
            (
                vec!["hello", "", "word"],
                0,
                None,
                vec!["hello", "", "word"],
            ),
            (vec!["hello", "", "word"], 1, None, vec!["ello", "", "ord"]),
            (vec!["hello", "", "word"], 2, None, vec!["llo", "", "rd"]),
            (vec!["hello", "", "word"], 3, None, vec!["lo", "", "d"]),
            (vec!["hello", "", "word"], 10, None, vec!["", "", ""]),
            // increase start negatively
            (vec!["hello", "", "word"], -1, None, vec!["o", "", "d"]),
            (vec!["hello", "", "word"], -2, None, vec!["lo", "", "rd"]),
            (vec!["hello", "", "word"], -3, None, vec!["llo", "", "ord"]),
            (
                vec!["hello", "", "word"],
                -10,
                None,
                vec!["hello", "", "word"],
            ),
            // increase length
            (vec!["hello", "", "word"], 1, Some(1), vec!["e", "", "o"]),
            (vec!["hello", "", "word"], 1, Some(2), vec!["el", "", "or"]),
            (
                vec!["hello", "", "word"],
                1,
                Some(3),
                vec!["ell", "", "ord"],
            ),
            (
                vec!["hello", "", "word"],
                1,
                Some(4),
                vec!["ello", "", "ord"],
            ),
            (vec!["hello", "", "word"], -3, Some(1), vec!["l", "", "o"]),
            (vec!["hello", "", "word"], -3, Some(2), vec!["ll", "", "or"]),
            (
                vec!["hello", "", "word"],
                -3,
                Some(3),
                vec!["llo", "", "ord"],
            ),
            (
                vec!["hello", "", "word"],
                -3,
                Some(4),
                vec!["llo", "", "ord"],
            ),
        ];

        cases
            .into_iter()
            .try_for_each::<_, Result<()>>(|(array, start, length, expected)| {
                let array = Utf8Array::<O>::from_slice(&array);
                let result = substring(&array, start, &length)?;
                assert_eq!(array.len(), result.len());
                let result = result.as_any().downcast_ref::<Utf8Array<O>>().unwrap();
                let expected = Utf8Array::<O>::from_slice(&expected);
                assert_eq!(&expected, result);
                Ok(())
            })?;

        Ok(())
    }

    #[test]
    fn without_nulls_string() -> Result<()> {
        without_nulls_utf8::<i32>()
    }

    #[test]
    fn without_nulls_large_string() -> Result<()> {
        without_nulls_utf8::<i64>()
    }

    fn with_null_binarys<O: Offset>() -> Result<()> {
        let cases = vec![
            // identity
            (
                vec![Some(b"hello"), None, Some(b"world")],
                0,
                None,
                vec![Some("hello"), None, Some("world")],
            ),
            // 0 length -> Nothing
            (
                vec![Some(b"hello"), None, Some(b"world")],
                0,
                Some(0),
                vec![Some(""), None, Some("")],
            ),
            // high start -> Nothing
            (
                vec![Some(b"hello"), None, Some(b"world")],
                1000,
                Some(0),
                vec![Some(""), None, Some("")],
            ),
            // high negative start -> identity
            (
                vec![Some(b"hello"), None, Some(b"world")],
                -1000,
                None,
                vec![Some("hello"), None, Some("world")],
            ),
            // high length -> identity
            (
                vec![Some(b"hello"), None, Some(b"world")],
                0,
                Some(1000),
                vec![Some("hello"), None, Some("world")],
            ),
        ];

        cases
            .into_iter()
            .try_for_each::<_, Result<()>>(|(array, start, length, expected)| {
                let array = BinaryArray::<O>::from(&array);
                let result = substring(&array, start, &length)?;
                assert_eq!(array.len(), result.len());

                let result = result.as_any().downcast_ref::<BinaryArray<O>>().unwrap();
                let expected = BinaryArray::<O>::from(&expected);
                assert_eq!(&expected, result);
                Ok(())
            })?;

        Ok(())
    }

    #[test]
    fn with_nulls_binary() -> Result<()> {
        with_null_binarys::<i32>()
    }

    #[test]
    fn with_nulls_large_binary() -> Result<()> {
        with_null_binarys::<i64>()
    }

    fn without_null_binarys<O: Offset>() -> Result<()> {
        let cases = vec![
            // increase start
            (
                vec!["hello", "", "word"],
                0,
                None,
                vec!["hello", "", "word"],
            ),
            (vec!["hello", "", "word"], 1, None, vec!["ello", "", "ord"]),
            (vec!["hello", "", "word"], 2, None, vec!["llo", "", "rd"]),
            (vec!["hello", "", "word"], 3, None, vec!["lo", "", "d"]),
            (vec!["hello", "", "word"], 10, None, vec!["", "", ""]),
            // increase start negatively
            (vec!["hello", "", "word"], -1, None, vec!["o", "", "d"]),
            (vec!["hello", "", "word"], -2, None, vec!["lo", "", "rd"]),
            (vec!["hello", "", "word"], -3, None, vec!["llo", "", "ord"]),
            (
                vec!["hello", "", "word"],
                -10,
                None,
                vec!["hello", "", "word"],
            ),
            // increase length
            (vec!["hello", "", "word"], 1, Some(1), vec!["e", "", "o"]),
            (vec!["hello", "", "word"], 1, Some(2), vec!["el", "", "or"]),
            (
                vec!["hello", "", "word"],
                1,
                Some(3),
                vec!["ell", "", "ord"],
            ),
            (
                vec!["hello", "", "word"],
                1,
                Some(4),
                vec!["ello", "", "ord"],
            ),
            (vec!["hello", "", "word"], -3, Some(1), vec!["l", "", "o"]),
            (vec!["hello", "", "word"], -3, Some(2), vec!["ll", "", "or"]),
            (
                vec!["hello", "", "word"],
                -3,
                Some(3),
                vec!["llo", "", "ord"],
            ),
            (
                vec!["hello", "", "word"],
                -3,
                Some(4),
                vec!["llo", "", "ord"],
            ),
        ];

        cases
            .into_iter()
            .try_for_each::<_, Result<()>>(|(array, start, length, expected)| {
                let array = BinaryArray::<O>::from_slice(&array);
                let result = substring(&array, start, &length)?;
                assert_eq!(array.len(), result.len());
                let result = result.as_any().downcast_ref::<BinaryArray<O>>().unwrap();
                let expected = BinaryArray::<O>::from_slice(&expected);
                assert_eq!(&expected, result);
                Ok(())
            })?;

        Ok(())
    }

    #[test]
    fn without_nulls_binary() -> Result<()> {
        without_null_binarys::<i32>()
    }

    #[test]
    fn without_nulls_large_binary() -> Result<()> {
        without_null_binarys::<i64>()
    }

    #[test]
    fn consistency() {
        use crate::array::new_null_array;
        use crate::datatypes::DataType::*;
        use crate::datatypes::TimeUnit;

        let datatypes = vec![
            Null,
            Boolean,
            UInt8,
            UInt16,
            UInt32,
            UInt64,
            Int8,
            Int16,
            Int32,
            Int64,
            Float32,
            Float64,
            Timestamp(TimeUnit::Second, None),
            Timestamp(TimeUnit::Millisecond, None),
            Timestamp(TimeUnit::Microsecond, None),
            Timestamp(TimeUnit::Nanosecond, None),
            Time64(TimeUnit::Microsecond),
            Time64(TimeUnit::Nanosecond),
            Date32,
            Time32(TimeUnit::Second),
            Time32(TimeUnit::Millisecond),
            Date64,
            Utf8,
            LargeUtf8,
            Binary,
            LargeBinary,
            Duration(TimeUnit::Second),
            Duration(TimeUnit::Millisecond),
            Duration(TimeUnit::Microsecond),
            Duration(TimeUnit::Nanosecond),
        ];

        datatypes.into_iter().for_each(|d1| {
            let array = new_null_array(d1.clone(), 10);
            if can_substring(&d1) {
                assert!(substring(array.as_ref(), 0, &None).is_ok());
            } else {
                assert!(substring(array.as_ref(), 0, &None).is_err());
            }
        });
    }
}
