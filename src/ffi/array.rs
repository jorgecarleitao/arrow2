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

use super::ffi::ArrowArrayRef;
use crate::array::{BooleanArray, FromFfi};
use crate::error::{ArrowError, Result};
use crate::types::days_ms;
use crate::{
    array::*,
    datatypes::{DataType, IntervalUnit},
};

/// Reads a valid `ffi` interface into a `Box<dyn Array>`
/// # Errors
/// If and only if:
/// * the data type is not supported
/// * the interface is not valid (e.g. a null pointer)
pub fn try_from<A: ArrowArrayRef>(array: A) -> Result<Box<dyn Array>> {
    let field = array.field()?;
    let array: Box<dyn Array> = match field.data_type() {
        DataType::Boolean => Box::new(BooleanArray::try_from_ffi(array)?),
        DataType::Int8 => Box::new(PrimitiveArray::<i8>::try_from_ffi(array)?),
        DataType::Int16 => Box::new(PrimitiveArray::<i16>::try_from_ffi(array)?),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            Box::new(PrimitiveArray::<i32>::try_from_ffi(array)?)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            Box::new(PrimitiveArray::<days_ms>::try_from_ffi(array)?)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => Box::new(PrimitiveArray::<i64>::try_from_ffi(array)?),
        DataType::Decimal(_, _) => Box::new(PrimitiveArray::<i128>::try_from_ffi(array)?),
        DataType::UInt8 => Box::new(PrimitiveArray::<u8>::try_from_ffi(array)?),
        DataType::UInt16 => Box::new(PrimitiveArray::<u16>::try_from_ffi(array)?),
        DataType::UInt32 => Box::new(PrimitiveArray::<u32>::try_from_ffi(array)?),
        DataType::UInt64 => Box::new(PrimitiveArray::<u64>::try_from_ffi(array)?),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => Box::new(PrimitiveArray::<f32>::try_from_ffi(array)?),
        DataType::Float64 => Box::new(PrimitiveArray::<f64>::try_from_ffi(array)?),
        DataType::Utf8 => Box::new(Utf8Array::<i32>::try_from_ffi(array)?),
        DataType::LargeUtf8 => Box::new(Utf8Array::<i64>::try_from_ffi(array)?),
        DataType::Binary => Box::new(BinaryArray::<i32>::try_from_ffi(array)?),
        DataType::LargeBinary => Box::new(BinaryArray::<i64>::try_from_ffi(array)?),
        DataType::List(_) => Box::new(ListArray::<i32>::try_from_ffi(array)?),
        DataType::LargeList(_) => Box::new(ListArray::<i64>::try_from_ffi(array)?),
        DataType::Struct(_) => Box::new(StructArray::try_from_ffi(array)?),
        DataType::Dictionary(keys, _) => match keys.as_ref() {
            DataType::Int8 => Box::new(DictionaryArray::<i8>::try_from_ffi(array)?),
            DataType::Int16 => Box::new(DictionaryArray::<i16>::try_from_ffi(array)?),
            DataType::Int32 => Box::new(DictionaryArray::<i32>::try_from_ffi(array)?),
            DataType::Int64 => Box::new(DictionaryArray::<i64>::try_from_ffi(array)?),
            DataType::UInt8 => Box::new(DictionaryArray::<u8>::try_from_ffi(array)?),
            DataType::UInt16 => Box::new(DictionaryArray::<u16>::try_from_ffi(array)?),
            DataType::UInt32 => Box::new(DictionaryArray::<u32>::try_from_ffi(array)?),
            DataType::UInt64 => Box::new(DictionaryArray::<u64>::try_from_ffi(array)?),
            _ => unreachable!(),
        },
        DataType::Union(_, _, _) => Box::new(UnionArray::try_from_ffi(array)?),
        data_type => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Reading DataType \"{}\" is not yet supported.",
                data_type
            )))
        }
    };

    Ok(array)
}
