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

use crate::{
    array::{
        growable::{Growable, GrowableList},
        Array, ListArray, Offset, PrimitiveArray,
    },
    error::Result,
};

use super::maybe_usize;
use super::Index;

/// `take` implementation for ListArrays
pub fn take<I: Offset, O: Index>(
    values: &ListArray<I>,
    indices: &PrimitiveArray<O>,
) -> Result<ListArray<I>> {
    let mut capacity = 0;
    let arrays = indices
        .values()
        .iter()
        .map(|i| {
            let index = maybe_usize::<O>(*i)?;
            let slice = values.slice(index, 1);
            capacity += slice.len();
            Ok(slice)
        })
        .collect::<Result<Vec<ListArray<I>>>>()?;

    let array_ref: Vec<&dyn Array> = arrays.iter().map(|v| v as &dyn Array).collect();

    if let Some(validity) = indices.validity() {
        let mut growable: GrowableList<I> = GrowableList::new(&array_ref, true, capacity);

        for index in 0..indices.len() {
            if validity.get_bit(index) {
                growable.extend(index, 0, 1);
            } else {
                growable.extend_validity(1)
            }
        }

        Ok(growable.into())
    } else {
        let mut growable: GrowableList<I> = GrowableList::new(&array_ref, false, capacity);
        for index in 0..indices.len() {
            growable.extend(index, 0, 1);
        }

        Ok(growable.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        array::{MutableListArray, MutablePrimitiveArray, PrimitiveArray, TryExtend},
        bitmap::Bitmap,
        buffer::Buffer,
        datatypes::DataType,
    };
    use std::sync::Arc;

    #[test]
    fn list_with_no_none() {
        let values = Buffer::from([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let values = PrimitiveArray::<i32>::from_data(DataType::Int32, values, None);

        let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
        let array = ListArray::<i32>::from_data(
            data_type,
            Buffer::from([0, 2, 2, 6, 9, 10]),
            Arc::new(values),
            None,
        );

        let indices = PrimitiveArray::from(&vec![Some(4i32), Some(1), Some(3)]).to(DataType::Int32);
        let result = take(&array, &indices).unwrap();

        let expected_values = Buffer::from([9, 6, 7, 8]);
        let expected_values =
            PrimitiveArray::<i32>::from_data(DataType::Int32, expected_values, None);
        let expected_type = ListArray::<i32>::default_datatype(DataType::Int32);
        let expected = ListArray::<i32>::from_data(
            expected_type,
            Buffer::from([0, 1, 1, 4]),
            Arc::new(expected_values),
            None,
        );

        assert_eq!(result, expected)
    }

    #[test]
    fn list_with_none() {
        let values = Buffer::from([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let values = PrimitiveArray::<i32>::from_data(DataType::Int32, values, None);

        let validity_values = vec![true, false, true, true, true];
        let validity = Bitmap::from_trusted_len_iter(validity_values.into_iter());

        let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
        let array = ListArray::<i32>::from_data(
            data_type,
            Buffer::from([0, 2, 2, 6, 9, 10]),
            Arc::new(values),
            Some(validity),
        );

        let indices =
            PrimitiveArray::from(&vec![Some(4i32), None, Some(2), Some(3)]).to(DataType::Int32);
        let result = take(&array, &indices).unwrap();

        let data_expected = vec![
            Some(vec![Some(9i32)]),
            None,
            Some(vec![Some(2i32), Some(3), Some(4), Some(5)]),
            Some(vec![Some(6i32), Some(7), Some(8)]),
        ];

        let mut expected = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
        expected.try_extend(data_expected).unwrap();
        let expected: ListArray<i32> = expected.into();

        assert_eq!(result, expected)
    }

    #[test]
    fn list_both_validity() {
        let values = vec![
            Some(vec![Some(2i32), Some(3), Some(4), Some(5)]),
            None,
            Some(vec![Some(9i32)]),
            Some(vec![Some(6i32), Some(7), Some(8)]),
        ];

        let mut array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
        array.try_extend(values).unwrap();
        let array: ListArray<i32> = array.into();

        let indices =
            PrimitiveArray::from(&vec![Some(3i32), None, Some(1), Some(0)]).to(DataType::Int32);
        let result = take(&array, &indices).unwrap();

        let data_expected = vec![
            Some(vec![Some(6i32), Some(7), Some(8)]),
            None,
            None,
            Some(vec![Some(2i32), Some(3), Some(4), Some(5)]),
        ];
        let mut expected = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
        expected.try_extend(data_expected).unwrap();
        let expected: ListArray<i32> = expected.into();

        assert_eq!(result, expected)
    }

    #[test]
    fn test_nested() {
        let values = Buffer::from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let values = PrimitiveArray::<i32>::from_data(DataType::Int32, values, None);

        let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
        let array = ListArray::<i32>::from_data(
            data_type,
            Buffer::from([0, 2, 4, 7, 7, 8, 10]),
            Arc::new(values),
            None,
        );

        let data_type = ListArray::<i32>::default_datatype(array.data_type().clone());
        let nested = ListArray::<i32>::from_data(
            data_type,
            Buffer::from([0, 2, 5, 6]),
            Arc::new(array),
            None,
        );

        let indices = PrimitiveArray::from(&vec![Some(0i32), Some(1)]).to(DataType::Int32);
        let result = take(&nested, &indices).unwrap();

        // expected data
        let expected_values = Buffer::from([1, 2, 3, 4, 5, 6, 7, 8]);
        let expected_values =
            PrimitiveArray::<i32>::from_data(DataType::Int32, expected_values, None);

        let expected_data_type = ListArray::<i32>::default_datatype(DataType::Int32);
        let expected_array = ListArray::<i32>::from_data(
            expected_data_type,
            Buffer::from([0, 2, 4, 7, 7, 8]),
            Arc::new(expected_values),
            None,
        );

        let expected_data_type =
            ListArray::<i32>::default_datatype(expected_array.data_type().clone());
        let expected_nested = ListArray::<i32>::from_data(
            expected_data_type,
            Buffer::from([0, 2, 5]),
            Arc::new(expected_array),
            None,
        );

        assert_eq!(result, expected_nested);
    }
}
