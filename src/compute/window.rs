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

//! Defines windowing functions, like `shift`ing

use crate::compute::concat;
use num::{abs, clamp};

use crate::{
    array::{new_null_array, Array},
    error::{ArrowError, Result},
};

/// Shifts array by defined number of items (to left or right)
/// A positive value for `offset` shifts the array to the right
/// a negative value shifts the array to the left.
/// # Examples
/// ```
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
/// use arrow2::compute::window::shift;
///
/// let array = Primitive::<i32>::from(&[Some(1), None, Some(3)]).to(DataType::Date32);
/// let result = shift(&array, -1).unwrap();
/// let expected = Primitive::<i32>::from(&[None, Some(3), None]).to(DataType::Date32);
/// assert_eq!(expected, result.as_ref());
/// ```
pub fn shift(array: &dyn Array, offset: i64) -> Result<Box<dyn Array>> {
    if abs(offset) as usize > array.len() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Shift's absolute offset must be smaller or equal to the arrays length. Offset is {}, length is {}",
            abs(offset), array.len()
        )));
    }

    // Compute slice
    let slice_offset = clamp(-offset, 0, array.len() as i64) as usize;
    let length = array.len() - abs(offset) as usize;
    let slice = array.slice(slice_offset, length);

    // Generate array with remaining `null` items
    let nulls = abs(offset as i64) as usize;

    let null_array = new_null_array(array.data_type().clone(), nulls);

    // Concatenate both arrays, add nulls after if shift > 0 else before
    if offset > 0 {
        concat::concatenate(&[null_array.as_ref(), slice.as_ref()])
    } else {
        concat::concatenate(&[slice.as_ref(), null_array.as_ref()])
    }
}

#[cfg(test)]
mod tests {
    use crate::array::Primitive;
    use crate::datatypes::DataType;

    use super::*;

    #[test]
    fn shift_pos() {
        let array = Primitive::<i32>::from(&[Some(1), None, Some(3)]).to(DataType::Date32);
        let result = shift(&array, 1).unwrap();

        let expected = Primitive::<i32>::from(&[None, Some(1), None]).to(DataType::Date32);

        assert_eq!(expected, result.as_ref());
    }

    #[test]
    fn shift_many() {
        let array = Primitive::<i32>::from(&[Some(1), None, Some(3)]).to(DataType::Date32);
        assert!(shift(&array, 10).is_err());
    }

    #[test]
    fn shift_max() {
        let array = Primitive::<i32>::from(&[Some(1), None, Some(3)]).to(DataType::Date32);
        let result = shift(&array, 3).unwrap();

        let expected = new_null_array(DataType::Date32, 3);

        assert_eq!(expected.as_ref(), result.as_ref());
    }
}
