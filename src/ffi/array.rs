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

//! Contains functionality to load an ArrayData from the C Data Interface

use std::convert::TryFrom;

use super::ffi::ArrowArray;
use crate::array::{BooleanArray, FromFFI};
use crate::error::{ArrowError, Result};
use crate::types::days_ms;
use crate::{
    array::{Array, PrimitiveArray},
    datatypes::{DataType, IntervalUnit},
};

impl TryFrom<ArrowArray> for Box<dyn Array> {
    type Error = ArrowError;

    fn try_from(array: ArrowArray) -> Result<Self> {
        let data_type = array.data_type()?;

        let array: Box<dyn Array> = match data_type {
            DataType::Boolean => Box::new(BooleanArray::try_from_ffi(data_type, array)?),
            DataType::Int8 => Box::new(PrimitiveArray::<i8>::try_from_ffi(data_type, array)?),
            DataType::Int16 => Box::new(PrimitiveArray::<i16>::try_from_ffi(data_type, array)?),
            DataType::Int32
            | DataType::Date32
            | DataType::Time32(_)
            | DataType::Interval(IntervalUnit::YearMonth) => {
                Box::new(PrimitiveArray::<i32>::try_from_ffi(data_type, array)?)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                Box::new(PrimitiveArray::<days_ms>::try_from_ffi(data_type, array)?)
            }
            DataType::Int64
            | DataType::Date64
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_) => {
                Box::new(PrimitiveArray::<i64>::try_from_ffi(data_type, array)?)
            }
            DataType::Decimal(_, _) => {
                Box::new(PrimitiveArray::<i128>::try_from_ffi(data_type, array)?)
            }
            DataType::UInt8 => Box::new(PrimitiveArray::<u8>::try_from_ffi(data_type, array)?),
            DataType::UInt16 => Box::new(PrimitiveArray::<u16>::try_from_ffi(data_type, array)?),
            DataType::UInt32 => Box::new(PrimitiveArray::<u32>::try_from_ffi(data_type, array)?),
            DataType::UInt64 => Box::new(PrimitiveArray::<u64>::try_from_ffi(data_type, array)?),
            DataType::Float16 => unreachable!(),
            DataType::Float32 => Box::new(PrimitiveArray::<f32>::try_from_ffi(data_type, array)?),
            DataType::Float64 => Box::new(PrimitiveArray::<f64>::try_from_ffi(data_type, array)?),
            _ => unimplemented!(),
        };

        Ok(array)
    }
}

impl TryFrom<Box<dyn Array>> for ArrowArray {
    type Error = ArrowError;

    fn try_from(array: Box<dyn Array>) -> Result<Self> {
        ArrowArray::try_new(array)
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{Array, Primitive};
    use crate::{datatypes::DataType, error::Result, ffi::ArrowArray};
    use std::convert::TryFrom;

    fn test_round_trip(expected: impl Array + Clone + 'static) -> Result<()> {
        // create a `ArrowArray` from the data.
        let b: Box<dyn Array> = Box::new(expected.clone());
        let d1 = ArrowArray::try_from(b)?;

        // here we export the array as 2 pointers. We would have no control over ownership if it was not for
        // the release mechanism.
        let (array, schema) = ArrowArray::into_raw(d1);

        // simulate an external consumer by being the consumer
        let d1 = unsafe { ArrowArray::try_from_raw(array, schema) }?;

        let result = Box::<dyn Array>::try_from(d1)?;

        let expected = Box::new(expected) as Box<dyn Array>;
        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_u32() -> Result<()> {
        let data = Primitive::<i32>::from(vec![Some(2), None, Some(1), None]).to(DataType::Int32);
        test_round_trip(data)
    }

    #[test]
    fn test_u64() -> Result<()> {
        let data = Primitive::<u64>::from(vec![Some(2), None, Some(1), None]).to(DataType::UInt64);
        test_round_trip(data)
    }

    #[test]
    fn test_i64() -> Result<()> {
        let data = Primitive::<i64>::from(vec![Some(2), None, Some(1), None]).to(DataType::Int64);
        test_round_trip(data)
    }
}
