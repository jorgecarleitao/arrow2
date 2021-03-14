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

use crate::types::NativeType;
use crate::{
    array::{Array, BooleanArray, ListArray, Offset, PrimitiveArray, Utf8Array},
    bitmap::Bitmap,
};
use crate::{
    datatypes::DataType,
    error::{ArrowError, Result},
};

use super::utils::combine_validities;

/// Checks if a [`GenericListArray`] contains a value in the [`PrimitiveArray`]
/// The validity will be equal to the `And` of both arrays.
fn contains_primitive<T, O>(list: &ListArray<O>, values: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: NativeType,
    O: Offset,
{
    if list.len() != values.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Contains requires arrays of the same length".to_string(),
        ));
    }
    if list.values().data_type() != values.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Contains requires the inner array to be of the same logical type".to_string(),
        ));
    }

    let validity = combine_validities(list.validity(), values.validity());

    let values = list.iter().zip(values.iter()).map(|(list, values)| {
        if list.is_none() | values.is_none() {
            // validity takes care of this
            return false;
        };
        let list = list.unwrap();
        let list = list.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        let values = values.unwrap();
        list.iter().any(|x| x.map(|x| x == values).unwrap_or(false))
    });
    let values = unsafe { Bitmap::from_trusted_len_iter(values) };

    Ok(BooleanArray::from_data(values, validity))
}

/// Checks if a [`GenericListArray`] contains a value in the [`Utf8Array`]
fn contains_utf8<O, OO>(list: &ListArray<O>, values: &Utf8Array<OO>) -> Result<BooleanArray>
where
    O: Offset,
    OO: Offset,
{
    if list.len() != values.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Contains requires arrays of the same length".to_string(),
        ));
    }
    if list.values().data_type() != values.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Contains requires the inner array to be of the same logical type".to_string(),
        ));
    }

    let validity = combine_validities(list.validity(), values.validity());

    let values = list.iter().zip(values.iter()).map(|(list, values)| {
        if list.is_none() | values.is_none() {
            // validity takes care of this
            return false;
        };
        let list = list.unwrap();
        let list = list.as_any().downcast_ref::<Utf8Array<OO>>().unwrap();
        let values = values.unwrap();
        list.iter().any(|x| x.map(|x| x == values).unwrap_or(false))
    });
    let values = unsafe { Bitmap::from_trusted_len_iter(values) };

    Ok(BooleanArray::from_data(values, validity))
}

macro_rules! primitive {
    ($list:expr, $values:expr, $l_ty:ty, $r_ty:ty) => {{
        let list = $list.as_any().downcast_ref::<ListArray<$l_ty>>().unwrap();
        let values = $values
            .as_any()
            .downcast_ref::<PrimitiveArray<$r_ty>>()
            .unwrap();
        contains_primitive(list, values)
    }};
}

pub fn contains(list: &dyn Array, values: &dyn Array) -> Result<BooleanArray> {
    let list_data_type = list.data_type();
    let values_data_type = values.data_type();

    match (list_data_type, values_data_type) {
        (DataType::List(_), DataType::Utf8) => {
            let list = list.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            let values = values.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            contains_utf8(list, values)
        }
        (DataType::List(_), DataType::LargeUtf8) => {
            let list = list.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            let values = values.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            contains_utf8(list, values)
        }
        (DataType::LargeList(_), DataType::LargeUtf8) => {
            let list = list.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            let values = values.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            contains_utf8(list, values)
        }
        (DataType::LargeList(_), DataType::Utf8) => {
            let list = list.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            let values = values.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            contains_utf8(list, values)
        }
        (DataType::List(_), DataType::Int8) => primitive!(list, values, i32, i8),
        (DataType::List(_), DataType::Int16) => primitive!(list, values, i32, i16),
        (DataType::List(_), DataType::Int32) => primitive!(list, values, i32, i32),
        (DataType::List(_), DataType::Int64) => primitive!(list, values, i32, i64),
        (DataType::List(_), DataType::UInt8) => primitive!(list, values, i32, u8),
        (DataType::List(_), DataType::UInt16) => primitive!(list, values, i32, u16),
        (DataType::List(_), DataType::UInt32) => primitive!(list, values, i32, u32),
        (DataType::List(_), DataType::UInt64) => primitive!(list, values, i32, u64),
        (DataType::List(_), DataType::Float32) => primitive!(list, values, i32, f32),
        (DataType::List(_), DataType::Float64) => primitive!(list, values, i32, f64),
        (DataType::LargeList(_), DataType::Int8) => primitive!(list, values, i64, i8),
        (DataType::LargeList(_), DataType::Int16) => primitive!(list, values, i64, i16),
        (DataType::LargeList(_), DataType::Int32) => primitive!(list, values, i64, i32),
        (DataType::LargeList(_), DataType::Int64) => primitive!(list, values, i64, i64),
        (DataType::LargeList(_), DataType::UInt8) => primitive!(list, values, i64, u8),
        (DataType::LargeList(_), DataType::UInt16) => primitive!(list, values, i64, u16),
        (DataType::LargeList(_), DataType::UInt32) => primitive!(list, values, i64, u32),
        (DataType::LargeList(_), DataType::UInt64) => primitive!(list, values, i64, u64),
        (DataType::LargeList(_), DataType::Float32) => primitive!(list, values, i64, f32),
        (DataType::LargeList(_), DataType::Float64) => primitive!(list, values, i64, f64),
        _ => Err(ArrowError::NotYetImplemented(format!(
            "Contains is not supported between logical types \"{:?}\" and \"{:?}\"",
            list_data_type, values_data_type
        ))),
    }
}

// disable wrapping inside literal vectors used for test data and assertions
#[rustfmt::skip::macros(vec)]
#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::*;

    // Expected behaviour:
    // contains([1, 2, null], 1) = true
    // contains([1, 2, null], 3) = false
    // contains([1, 2, null], null) = null
    // contains(null, 1) = null
    #[test]
    fn test_contains() {
        let data = vec![
            Some(vec![Some(1), Some(2), None]),
            Some(vec![Some(1), Some(2), None]),
            Some(vec![Some(1), Some(2), None]),
            None,
        ];
        let values = Primitive::<i32>::from(vec![
            Some(1),
            Some(3),
            None,
            Some(1),
            ])
        .to(DataType::Int32);
        let expected = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            None
        ]);

        let a: ListPrimitive<i32, Primitive<i32>, i32> = data.into_iter().collect();
        let a = a.to(ListArray::<i32>::default_datatype(DataType::Int32));

        let result = contains(&a, &values).unwrap();

        assert_eq!(result, expected);
    }
}
