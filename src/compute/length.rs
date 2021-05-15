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

//! Defines kernel for length of composite arrays

use crate::{
    array::*,
    buffer::Buffer,
    datatypes::DataType,
    error::{ArrowError, Result},
};

fn unary_offsets_string<O, F>(array: &Utf8Array<O>, op: F) -> PrimitiveArray<O>
where
    O: Offset,
    F: Fn(O) -> O,
{
    let values = array
        .offsets()
        .windows(2)
        .map(|offset| op(offset[1] - offset[0]));

    let values = Buffer::from_trusted_len_iter(values);

    let data_type = if O::is_large() {
        DataType::Int64
    } else {
        DataType::Int32
    };

    PrimitiveArray::<O>::from_data(data_type, values, array.validity().clone())
}

/// Returns an array of integers with the number of bytes on each string of the array.
pub fn length(array: &dyn Array) -> Result<Box<dyn Array>> {
    match array.data_type() {
        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            Ok(Box::new(unary_offsets_string::<i32, _>(array, |x| x)))
        }
        DataType::LargeUtf8 => {
            let array = array.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            Ok(Box::new(unary_offsets_string::<i64, _>(array, |x| x)))
        }
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "length not supported for {:?}",
            array.data_type()
        ))),
    }
}

/// Checks if an array of type `datatype` can perform lenght operation
///
/// # Examples
/// ```
/// use arrow2::compute::length::can_length;
/// use arrow2::datatypes::{DataType};
///
/// let data_type = DataType::Utf8;
/// assert_eq!(can_length(&data_type), true);

/// let data_type = DataType::Int8;
/// assert_eq!(can_length(&data_type), false);
/// ```
pub fn can_length(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn length_test_string<O: Offset>() {
        vec![
            (
                vec![Some("hello"), Some(" "), None],
                vec![Some(5usize), Some(1), None],
            ),
            (vec![Some("💖")], vec![Some(4)]),
        ]
        .into_iter()
        .for_each(|(input, expected)| {
            let array = Utf8Array::<O>::from(&input);
            let result = length(&array).unwrap();

            let data_type = if O::is_large() {
                DataType::Int64
            } else {
                DataType::Int32
            };

            let expected = expected
                .into_iter()
                .map(|x| x.map(|x| O::from_usize(x).unwrap()))
                .collect::<Primitive<O>>()
                .to(data_type);
            assert_eq!(expected, result.as_ref());
        })
    }

    #[test]
    fn large_utf8() {
        length_test_string::<i64>()
    }

    #[test]
    fn utf8() {
        length_test_string::<i32>()
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

        datatypes.clone().into_iter().for_each(|d1| {
            let array = new_null_array(d1.clone(), 10);
            if can_length(&d1) {
                assert!(length(array.as_ref()).is_ok());
            } else {
                assert!(length(array.as_ref()).is_err());
            }
        });
    }
}
