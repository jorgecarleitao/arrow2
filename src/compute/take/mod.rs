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
        new_empty_array, Array, BinaryArray, BooleanArray, DictionaryArray, ListArray, NullArray,
        PrimitiveArray, StructArray, Utf8Array,
    },
    datatypes::{DataType, IntervalUnit},
    error::{ArrowError, Result},
};

pub use crate::array::Index;

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

pub fn take<O: Index>(values: &dyn Array, indices: &PrimitiveArray<O>) -> Result<Box<dyn Array>> {
    if indices.len() == 0 {
        return Ok(new_empty_array(values.data_type().clone()));
    }

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

/// Checks if an array of type `datatype` can perform take operation
///
/// # Examples
/// ```
/// use arrow2::compute::take::can_take;
/// use arrow2::datatypes::{DataType};
///
/// let data_type = DataType::Int8;
/// assert_eq!(can_take(&data_type), true);
/// ```
pub fn can_take(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth)
        | DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Timestamp(_, _)
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal(_, _)
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_) => true,
        DataType::Dictionary(key_type, _) => matches!(
            key_type.as_ref(),
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        ),
        _ => false,
    }
}

#[inline(always)]
fn maybe_usize<I: Index>(index: I) -> Result<usize> {
    index
        .to_usize()
        .ok_or(ArrowError::DictionaryKeyOverflowError)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::datatypes::Field;
    use crate::{
        array::{Int32Array, Primitive, UInt32Array},
        bitmap::MutableBitmap,
        types::NativeType,
    };

    use super::*;

    fn test_take_primitive<T>(
        data: &[Option<T>],
        indices: &Int32Array,
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
        let indices = Int32Array::from_slice(&[0, 5, 3, 1, 4, 2]);
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
        let indices = Int32Array::from(&[Some(0), None, Some(3), Some(1), Some(4), Some(2)]);
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

        let indices = Int32Array::from(&[None, Some(3), Some(1), None, Some(0)]);

        let output = take(&array, &indices).unwrap();

        let boolean = BooleanArray::from(&[None, Some(true), Some(false), None, Some(true)]);
        let int = Int32Array::from(&[None, Some(31), Some(28), None, Some(42)]);
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
            if can_take(&d1) {
                let indices =
                    Primitive::<i32>::from(&[Some(1), Some(2), None, Some(3)]).to(DataType::Int32);
                assert!(take(array.as_ref(), &indices).is_ok());
            } else {
                let indices =
                    Primitive::<i32>::from(&[Some(1), Some(2), None, Some(3)]).to(DataType::Int32);
                assert!(take(array.as_ref(), &indices).is_err());
            }
        });
    }

    #[test]
    fn empty() {
        let indices = Int32Array::from_slice(&[]);
        let values = BooleanArray::from(vec![Some(true), Some(false)]);
        let a = take(&values, &indices).unwrap();
        assert_eq!(a.len(), 0)
    }

    #[test]
    fn unsigned_take() {
        let indices = UInt32Array::from_slice(&[]);
        let values = BooleanArray::from(vec![Some(true), Some(false)]);
        let a = take(&values, &indices).unwrap();
        assert_eq!(a.len(), 0)
    }
}
