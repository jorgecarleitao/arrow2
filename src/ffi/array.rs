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
use crate::{array::*, datatypes::PhysicalType};

/// Reads a valid `ffi` interface into a `Box<dyn Array>`
/// # Errors
/// If and only if:
/// * the data type is not supported
/// * the interface is not valid (e.g. a null pointer)
pub fn try_from<A: ArrowArrayRef>(array: A) -> Result<Box<dyn Array>> {
    use PhysicalType::*;
    Ok(match array.field().data_type().to_physical_type() {
        Boolean => Box::new(BooleanArray::try_from_ffi(array)?),
        Int8 => Box::new(PrimitiveArray::<i8>::try_from_ffi(array)?),
        Int16 => Box::new(PrimitiveArray::<i16>::try_from_ffi(array)?),
        Int32 => Box::new(PrimitiveArray::<i32>::try_from_ffi(array)?),
        DaysMs => Box::new(PrimitiveArray::<days_ms>::try_from_ffi(array)?),
        Int64 => Box::new(PrimitiveArray::<i64>::try_from_ffi(array)?),
        Int128 => Box::new(PrimitiveArray::<i128>::try_from_ffi(array)?),
        UInt8 => Box::new(PrimitiveArray::<u8>::try_from_ffi(array)?),
        UInt16 => Box::new(PrimitiveArray::<u16>::try_from_ffi(array)?),
        UInt32 => Box::new(PrimitiveArray::<u32>::try_from_ffi(array)?),
        UInt64 => Box::new(PrimitiveArray::<u64>::try_from_ffi(array)?),
        Float32 => Box::new(PrimitiveArray::<f32>::try_from_ffi(array)?),
        Float64 => Box::new(PrimitiveArray::<f64>::try_from_ffi(array)?),
        Utf8 => Box::new(Utf8Array::<i32>::try_from_ffi(array)?),
        LargeUtf8 => Box::new(Utf8Array::<i64>::try_from_ffi(array)?),
        Binary => Box::new(BinaryArray::<i32>::try_from_ffi(array)?),
        LargeBinary => Box::new(BinaryArray::<i64>::try_from_ffi(array)?),
        List => Box::new(ListArray::<i32>::try_from_ffi(array)?),
        LargeList => Box::new(ListArray::<i64>::try_from_ffi(array)?),
        Struct => Box::new(StructArray::try_from_ffi(array)?),
        Dictionary(key_type) => {
            with_match_physical_dictionary_key_type!(key_type, |$T| {
                Box::new(DictionaryArray::<$T>::try_from_ffi(array)?)
            })
        }
        Union => Box::new(UnionArray::try_from_ffi(array)?),
        data_type => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Importing PhysicalType \"{:?}\" is not yet supported.",
                data_type
            )))
        }
    })
}
