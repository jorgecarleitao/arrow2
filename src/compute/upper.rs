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

//! Defines kernel to extract a upper case of a \[Large\]StringArray

use super::utils::utf8_apply;
use crate::array::*;
use crate::{
    datatypes::DataType,
    error::{ArrowError, Result},
};

/// Returns a new `Array` where each of each of the elements is upper-cased.
/// this function errors when the passed array is not a \[Large\]String array.
pub fn upper(array: &dyn Array) -> Result<Box<dyn Array>> {
    match array.data_type() {
        DataType::LargeUtf8 => Ok(Box::new(utf8_apply(
            str::to_uppercase,
            array
                .as_any()
                .downcast_ref::<Utf8Array<i64>>()
                .expect("A large string is expected"),
        ))),
        DataType::Utf8 => Ok(Box::new(utf8_apply(
            str::to_uppercase,
            array
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .expect("A string is expected"),
        ))),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "upper does not support type {:?}",
            array.data_type()
        ))),
    }
}

/// Checks if an array of type `datatype` can perform upper operation
///
/// # Examples
/// ```
/// use arrow2::compute::upper::can_upper;
/// use arrow2::datatypes::{DataType};
///
/// let data_type = DataType::Utf8;
/// assert_eq!(can_upper(&data_type), true);
///
/// let data_type = DataType::Null;
/// assert_eq!(can_upper(&data_type), false);
/// ```
pub fn can_upper(data_type: &DataType) -> bool {
    matches!(data_type, DataType::LargeUtf8 | DataType::Utf8)
}
