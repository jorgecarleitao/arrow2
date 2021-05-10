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

//! Defines take kernel for [`Array`]

use crate::{
    array::{
        Array, BinaryArray, BooleanArray, DictionaryArray, ListArray, NullArray, Offset,
        PrimitiveArray, StructArray, Utf8Array,
    },
    datatypes::{DataType, IntervalUnit},
    error::{ArrowError, Result},
};

mod binary;
mod boolean;
mod dict;
mod generic_binary;
mod list;
mod primitive;
mod structure;
mod utf8;

macro_rules! downcast_take {
    ($type: ty, $values: expr, $indices: expr) => {{
        let values = $values
            .as_any()
            .downcast_ref::<PrimitiveArray<$type>>()
            .expect("Unable to downcast to a primitive array");
        Ok(Box::new(primitive::take::<$type, _>(&values, $indices)?))
    }};
}

macro_rules! downcast_dict_take {
    ($type: ty, $values: expr, $indices: expr) => {{
        let values = $values
            .as_any()
            .downcast_ref::<DictionaryArray<$type>>()
            .expect("Unable to downcast to a primitive array");
        Ok(Box::new(dict::take::<$type, _>(&values, $indices)?))
    }};
}

pub fn take<O: Offset>(values: &dyn Array, indices: &PrimitiveArray<O>) -> Result<Box<dyn Array>> {
    match values.data_type() {
        DataType::Null => Ok(Box::new(NullArray::from_data(indices.len()))),
        DataType::Boolean => {
            let values = values.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(Box::new(boolean::take::<O>(values, indices)?))
        }
        DataType::Int8 => downcast_take!(i8, values, indices),
        DataType::Int16 => downcast_take!(i16, values, indices),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => downcast_take!(i32, values, indices),
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Timestamp(_, _) => downcast_take!(i64, values, indices),
        DataType::UInt8 => downcast_take!(u8, values, indices),
        DataType::UInt16 => downcast_take!(u16, values, indices),
        DataType::UInt32 => downcast_take!(u32, values, indices),
        DataType::UInt64 => downcast_take!(u64, values, indices),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => downcast_take!(f32, values, indices),
        DataType::Float64 => downcast_take!(f64, values, indices),
        DataType::Decimal(_, _) => downcast_take!(i128, values, indices),
        DataType::Utf8 => {
            let values = values.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            Ok(Box::new(utf8::take::<i32, _>(values, indices)?))
        }
        DataType::LargeUtf8 => {
            let values = values.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            Ok(Box::new(utf8::take::<i64, _>(values, indices)?))
        }
        DataType::Binary => {
            let values = values.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();
            Ok(Box::new(binary::take::<i32, _>(values, indices)?))
        }
        DataType::LargeBinary => {
            let values = values.as_any().downcast_ref::<BinaryArray<i64>>().unwrap();
            Ok(Box::new(binary::take::<i64, _>(values, indices)?))
        }
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::Int8 => downcast_dict_take!(i8, values, indices),
            DataType::Int16 => downcast_dict_take!(i16, values, indices),
            DataType::Int32 => downcast_dict_take!(i32, values, indices),
            DataType::Int64 => downcast_dict_take!(i64, values, indices),
            DataType::UInt8 => downcast_dict_take!(u8, values, indices),
            DataType::UInt16 => downcast_dict_take!(u16, values, indices),
            DataType::UInt32 => downcast_dict_take!(u32, values, indices),
            DataType::UInt64 => downcast_dict_take!(u64, values, indices),
            _ => unreachable!(),
        },
        DataType::Struct(_) => {
            let array = values.as_any().downcast_ref::<StructArray>().unwrap();
            Ok(Box::new(structure::take::<_>(array, indices)?))
        }
        DataType::List(_) => {
            let array = values.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            Ok(Box::new(list::take::<i32, O>(array, indices)?))
        }
        DataType::LargeList(_) => {
            let array = values.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            Ok(Box::new(list::take::<i64, O>(array, indices)?))
        }
        t => unimplemented!("Take not supported for data type {:?}", t),
    }
}

#[inline(always)]
fn maybe_usize<I: Offset>(index: I) -> Result<usize> {
    index
        .to_usize()
        .ok_or(ArrowError::DictionaryKeyOverflowError)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::datatypes::Field;
    use crate::{array::Primitive, bitmap::MutableBitmap, types::NativeType};

    use super::*;

    fn test_take_primitive<T>(
        data: &[Option<T>],
        indices: &PrimitiveArray<i32>,
        expected_data: &[Option<T>],
        data_type: DataType,
    ) -> Result<()>
    where
        T: NativeType,
    {
        let output = Primitive::<T>::from(data).to(data_type.clone());
        let expected = Primitive::<T>::from(expected_data).to(data_type);
        let output = take(&output, indices)?;
        assert_eq!(expected, output.as_ref());
        Ok(())
    }

    #[test]
    fn test_take_primitive_non_null_indices() {
        let indices = Primitive::<i32>::from_slice(&[0, 5, 3, 1, 4, 2]).to(DataType::Int32);
        test_take_primitive::<i8>(
            &[None, Some(2), Some(4), Some(6), Some(8), None],
            &indices,
            &[None, None, Some(6), Some(2), Some(8), Some(4)],
            DataType::Int8,
        )
        .unwrap();

        test_take_primitive::<i8>(
            &[Some(0), Some(2), Some(4), Some(6), Some(8), Some(10)],
            &indices,
            &[Some(0), Some(10), Some(6), Some(2), Some(8), Some(4)],
            DataType::Int8,
        )
        .unwrap();
    }

    #[test]
    fn test_take_primitive_null_values() {
        let indices = Primitive::<i32>::from(&[Some(0), None, Some(3), Some(1), Some(4), Some(2)])
            .to(DataType::Int32);
        test_take_primitive::<i8>(
            &[Some(0), Some(2), Some(4), Some(6), Some(8), Some(10)],
            &indices,
            &[Some(0), None, Some(6), Some(2), Some(8), Some(4)],
            DataType::Int8,
        )
        .unwrap();

        test_take_primitive::<i8>(
            &[None, Some(2), Some(4), Some(6), Some(8), Some(10)],
            &indices,
            &[None, None, Some(6), Some(2), Some(8), Some(4)],
            DataType::Int8,
        )
        .unwrap();
    }

    fn test_take_utf8<O>(
        data: &[Option<&str>],
        indices: &PrimitiveArray<i32>,
        expected_data: &[Option<&str>],
    ) -> Result<()>
    where
        O: Offset,
    {
        let output = Utf8Array::<O>::from(&data.to_vec());
        let expected = Utf8Array::<O>::from(&expected_data.to_vec());
        let output = take(&output, indices)?;
        assert_eq!(expected, output.as_ref());
        Ok(())
    }

    #[test]
    fn test_utf8_nulls_nulls() {
        let index =
            Primitive::<i32>::from(&[Some(3), None, Some(1), Some(3), Some(4)]).to(DataType::Int32);
        test_take_utf8::<i32>(
            &[Some("one"), None, Some("three"), Some("four"), Some("five")],
            &index,
            &[Some("four"), None, None, Some("four"), Some("five")],
        )
        .unwrap();
    }

    fn test_take_boolean_arrays(
        data: &[Option<bool>],
        indices: &PrimitiveArray<i32>,
        expected_data: &[Option<bool>],
    ) {
        let input = BooleanArray::from(data);
        let expected = BooleanArray::from(expected_data);
        let output = take(&input, indices).unwrap();
        assert_eq!(expected, output.as_ref());
    }

    #[test]
    fn test_boolean() {
        let index =
            Primitive::<i32>::from(&[Some(3), None, Some(1), Some(3), Some(2)]).to(DataType::Int32);

        test_take_boolean_arrays(
            &[Some(false), None, Some(true), Some(false), None],
            &index,
            &[Some(false), None, None, Some(false), Some(true)],
        );

        test_take_boolean_arrays(
            &[Some(false), Some(true), Some(true), Some(false), Some(true)],
            &index,
            &[Some(false), None, Some(true), Some(false), Some(true)],
        );

        let index = Primitive::<i32>::from(&[Some(3), Some(0), Some(1), Some(3), Some(2)])
            .to(DataType::Int32);

        test_take_boolean_arrays(
            &[Some(false), Some(true), Some(true), Some(false), Some(true)],
            &index,
            &[
                Some(false),
                Some(false),
                Some(true),
                Some(false),
                Some(true),
            ],
        );

        test_take_boolean_arrays(
            &[Some(false), None, Some(true), Some(false), Some(true)],
            &index,
            &[Some(false), Some(false), None, Some(false), Some(true)],
        );
    }

    fn create_test_struct() -> StructArray {
        let boolean = BooleanArray::from_slice(&[true, false, false, true]);
        let int = Primitive::from_slice(&[42, 28, 19, 31]).to(DataType::Int32);
        let validity = vec![true, true, false, true]
            .into_iter()
            .collect::<MutableBitmap>()
            .into();
        let fields = vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Int32, true),
        ];
        StructArray::from_data(
            fields,
            vec![
                Arc::new(boolean) as Arc<dyn Array>,
                Arc::new(int) as Arc<dyn Array>,
            ],
            validity,
        )
    }

    #[test]
    fn test_struct_with_nulls() {
        let array = create_test_struct();

        let indices =
            Primitive::<i32>::from(&[None, Some(3), Some(1), None, Some(0)]).to(DataType::Int32);

        let output = take(&array, &indices).unwrap();

        let boolean = BooleanArray::from(&[None, Some(true), Some(false), None, Some(true)]);
        let int = Primitive::from(&[None, Some(31), Some(28), None, Some(42)]).to(DataType::Int32);
        let validity = vec![false, true, true, false, true]
            .into_iter()
            .collect::<MutableBitmap>()
            .into();
        let expected = StructArray::from_data(
            array.fields().to_vec(),
            vec![
                Arc::new(boolean) as Arc<dyn Array>,
                Arc::new(int) as Arc<dyn Array>,
            ],
            validity,
        );
        assert_eq!(expected, output.as_ref());
    }
}
