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

//! Defines concat kernel for `ArrayRef`
//!
//! Example:
//!
//! ```
//! use arrow2::array::Utf8Array;
//! use arrow2::compute::concat::concatenate;
//!
//! let arr = concatenate(&[
//!     &Utf8Array::<i32>::from_slice(vec!["hello", "world"]),
//!     &Utf8Array::<i32>::from_slice(vec!["!"]),
//! ]).unwrap();
//! assert_eq!(arr.len(), 3);
//! ```

use crate::array::{growable::make_growable, Array};
use crate::error::{ArrowError, Result};

/// Concatenate multiple [Array] of the same type into a single [ArrayRef].
pub fn concatenate(arrays: &[&dyn Array]) -> Result<Box<dyn Array>> {
    if arrays.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "concat requires input of at least one array".to_string(),
        ));
    }

    if arrays
        .iter()
        .any(|array| array.data_type() != arrays[0].data_type())
    {
        return Err(ArrowError::InvalidArgumentError(
            "It is not possible to concatenate arrays of different data types.".to_string(),
        ));
    }

    let lengths = arrays.iter().map(|array| array.len()).collect::<Vec<_>>();
    let capacity = lengths.iter().sum();

    let mut mutable = make_growable(arrays, false, capacity);

    for (i, len) in lengths.iter().enumerate() {
        mutable.extend(i, 0, *len)
    }

    Ok(mutable.to_box())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::*;
    use crate::datatypes::*;

    #[test]
    fn test_concat_empty_vec() {
        let re = concatenate(&[]);
        assert!(re.is_err());
    }

    #[test]
    fn test_concat_incompatible_datatypes() {
        let re = concatenate(&[
            &Primitive::<i64>::from(vec![Some(-1), Some(2), None]).to(DataType::Int64),
            &Utf8Array::<i32>::from(&vec![Some("hello"), Some("bar"), Some("world")]),
        ]);
        assert!(re.is_err());
    }

    #[test]
    fn test_concat_string_arrays() -> Result<()> {
        let arr = concatenate(&[
            &Utf8Array::<i32>::from_slice(&vec!["hello", "world"]),
            &Utf8Array::<i32>::from_slice(&vec!["2", "3", "4"]),
            &Utf8Array::<i32>::from(&vec![Some("foo"), Some("bar"), None, Some("baz")]),
        ])?;

        let expected_output = Utf8Array::<i32>::from(&vec![
            Some("hello"),
            Some("world"),
            Some("2"),
            Some("3"),
            Some("4"),
            Some("foo"),
            Some("bar"),
            None,
            Some("baz"),
        ]);

        assert_eq!(expected_output, arr.as_ref());

        Ok(())
    }

    #[test]
    fn test_concat_primitive_arrays() -> Result<()> {
        let arr = concatenate(&[
            &Primitive::<i64>::from(vec![Some(-1), Some(-1), Some(2), None, None])
                .to(DataType::Int64),
            &Primitive::<i64>::from(vec![Some(101), Some(102), Some(103), None])
                .to(DataType::Int64),
            &Primitive::<i64>::from(vec![Some(256), Some(512), Some(1024)]).to(DataType::Int64),
        ])?;

        let expected_output = Primitive::<i64>::from(vec![
            Some(-1),
            Some(-1),
            Some(2),
            None,
            None,
            Some(101),
            Some(102),
            Some(103),
            None,
            Some(256),
            Some(512),
            Some(1024),
        ])
        .to(DataType::Int64);

        assert_eq!(expected_output, arr.as_ref());

        Ok(())
    }

    #[test]
    fn test_concat_primitive_array_slices() -> Result<()> {
        let input_1 = Primitive::<i64>::from(vec![Some(-1), Some(-1), Some(2), None, None])
            .to(DataType::Int64)
            .slice(1, 3);

        let input_2 = Primitive::<i64>::from(vec![Some(101), Some(102), Some(103), None])
            .to(DataType::Int64)
            .slice(1, 3);
        let arr = concatenate(&[&input_1, &input_2])?;

        let expected_output =
            Primitive::<i64>::from(vec![Some(-1), Some(2), None, Some(102), Some(103), None])
                .to(DataType::Int64);

        assert_eq!(expected_output, arr.as_ref());

        Ok(())
    }

    #[test]
    fn test_concat_boolean_primitive_arrays() -> Result<()> {
        let arr = concatenate(&[
            &BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                None,
                None,
                Some(false),
            ]),
            &BooleanArray::from(vec![None, Some(false), Some(true), Some(false)]),
        ])?;

        let expected_output = BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(false),
            None,
            None,
            Some(false),
            None,
            Some(false),
            Some(true),
            Some(false),
        ]);

        assert_eq!(expected_output, arr.as_ref());

        Ok(())
    }

    // todo: migrate me
    /*
    #[test]
    fn test_concat_primitive_list_arrays() -> Result<()> {
        let list1 = vec![
            Some(vec![Some(-1), Some(-1), Some(2), None, None]),
            Some(vec![]),
            None,
            Some(vec![Some(10)]),
        ];
        let list1_array =
            ListArray::from_iter_primitive::<i64, _, _>(list1.clone());

        let list2 = vec![
            None,
            Some(vec![Some(100), None, Some(101)]),
            Some(vec![Some(102)]),
        ];
        let list2_array =
            ListArray::from_iter_primitive::<i64, _, _>(list2.clone());

        let list3 = vec![Some(vec![Some(1000), Some(1001)])];
        let list3_array =
            ListArray::from_iter_primitive::<i64, _, _>(list3.clone());

        let array_result = concatenate(&[&list1_array, &list2_array, &list3_array])?;

        let expected = list1
            .into_iter()
            .chain(list2.into_iter())
            .chain(list3.into_iter());
        let array_expected = ListArray::from_iter_primitive::<i64, _, _>(expected);

        assert_eq!(array_result.as_ref(), &array_expected as &dyn Array);

        Ok(())
    }

    #[test]
    fn test_concat_struct_arrays() -> Result<()> {
        let field = Field::new("field", DataType::Int64, true);
        let input_primitive_1: ArrayRef =
            Arc::new(PrimitiveArray::<i64>::from(vec![
                Some(-1),
                Some(-1),
                Some(2),
                None,
                None,
            ]));
        let input_struct_1 = StructArray::from(vec![(field.clone(), input_primitive_1)]);

        let input_primitive_2: ArrayRef =
            Arc::new(PrimitiveArray::<i64>::from(vec![
                Some(101),
                Some(102),
                Some(103),
                None,
            ]));
        let input_struct_2 = StructArray::from(vec![(field.clone(), input_primitive_2)]);

        let input_primitive_3: ArrayRef =
            Arc::new(PrimitiveArray::<i64>::from(vec![
                Some(256),
                Some(512),
                Some(1024),
            ]));
        let input_struct_3 = StructArray::from(vec![(field, input_primitive_3)]);

        let arr = concatenate(&[&input_struct_1, &input_struct_2, &input_struct_3])?;

        let expected_primitive_output = Arc::new(PrimitiveArray::<i64>::from(vec![
            Some(-1),
            Some(-1),
            Some(2),
            None,
            None,
            Some(101),
            Some(102),
            Some(103),
            None,
            Some(256),
            Some(512),
            Some(1024),
        ])) as ArrayRef;

        let actual_primitive = arr
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column(0);
        assert_eq!(actual_primitive, &expected_primitive_output);

        Ok(())
    }

    #[test]
    fn test_concat_struct_array_slices() -> Result<()> {
        let field = Field::new("field", DataType::Int64, true);
        let input_primitive_1: ArrayRef =
            Arc::new(PrimitiveArray::<i64>::from(vec![
                Some(-1),
                Some(-1),
                Some(2),
                None,
                None,
            ]));
        let input_struct_1 = StructArray::from(vec![(field.clone(), input_primitive_1)]);

        let input_primitive_2: ArrayRef =
            Arc::new(PrimitiveArray::<i64>::from(vec![
                Some(101),
                Some(102),
                Some(103),
                None,
            ]));
        let input_struct_2 = StructArray::from(vec![(field, input_primitive_2)]);

        let arr = concatenate(&[
            input_struct_1.slice(1, 3).as_ref(),
            input_struct_2.slice(1, 2).as_ref(),
        ])?;

        let expected_primitive_output = Arc::new(PrimitiveArray::<i64>::from(vec![
            Some(-1),
            Some(2),
            None,
            Some(102),
            Some(103),
        ])) as ArrayRef;

        let actual_primitive = arr
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column(0);
        assert_eq!(actual_primitive, &expected_primitive_output);

        Ok(())
    }

    #[test]
    fn test_string_array_slices() -> Result<()> {
        let input_1 = StringArray::from(vec!["hello", "A", "B", "C"]);
        let input_2 = StringArray::from(vec!["world", "D", "E", "Z"]);

        let arr = concatenate(&[input_1.slice(1, 3).as_ref(), input_2.slice(1, 2).as_ref()])?;

        let expected_output = StringArray::from(vec!["A", "B", "C", "D", "E"]);

        let actual_output = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(actual_output, &expected_output);

        Ok(())
    }

    #[test]
    fn test_string_array_with_null_slices() -> Result<()> {
        let input_1 = StringArray::from(vec![Some("hello"), None, Some("A"), Some("C")]);
        let input_2 = StringArray::from(vec![None, Some("world"), Some("D"), None]);

        let arr = concatenate(&[input_1.slice(1, 3).as_ref(), input_2.slice(1, 2).as_ref()])?;

        let expected_output =
            StringArray::from(vec![None, Some("A"), Some("C"), Some("world"), Some("D")]);

        let actual_output = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(actual_output, &expected_output);

        Ok(())
    }
     */
}
