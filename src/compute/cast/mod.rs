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
    array::*,
    buffer::Buffer,
    temporal_conversions::{MILLISECONDS, MILLISECONDS_IN_DAY},
};
use crate::{
    datatypes::*,
    temporal_conversions::{MICROSECONDS, NANOSECONDS},
};
use crate::{
    error::{ArrowError, Result},
    temporal_conversions::SECONDS_IN_DAY,
};

use self::{
    boolean_to::{cast_bool_to_numeric, cast_bool_to_utf8},
    dictionary_to::dictionary_cast,
    primitive_to::{
        cast_array_data, cast_numeric_arrays, cast_numeric_to_bool, cast_numeric_to_string,
        primitive_to_dictionary,
    },
    string_to::{cast_string_to_numeric, string_to_dictionary, to_date32, to_date64},
};

use super::arity::unary;

mod boolean_to;
mod dictionary_to;
mod primitive_to;
mod string_to;

/// Returns true if this type is numeric: (UInt*, Unit*, or Float*).
fn is_numeric(t: &DataType) -> bool {
    use DataType::*;
    matches!(
        t,
        UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64 | Float32 | Float64
    )
}

/// Return true if a value of type `from_type` can be cast into a
/// value of `to_type`. Note that such as cast may be lossy.
///
/// If this function returns true to stay consistent with the `cast` kernel below.
pub fn can_cast_types(from_type: &DataType, to_type: &DataType) -> bool {
    use self::DataType::*;
    if from_type == to_type {
        return true;
    }

    match (from_type, to_type) {
        (Struct(_), _) => false,
        (_, Struct(_)) => false,
        (List(list_from), List(list_to)) => {
            can_cast_types(list_from.data_type(), list_to.data_type())
        }
        (List(_), _) => false,
        (_, List(list_to)) => can_cast_types(from_type, list_to.data_type()),
        (Dictionary(_, from_value_type), Dictionary(_, to_value_type)) => {
            can_cast_types(from_value_type, to_value_type)
        }
        (Dictionary(_, value_type), _) => can_cast_types(value_type, to_type),
        (_, Dictionary(_, value_type)) => can_cast_types(from_type, value_type),

        (_, Boolean) => is_numeric(from_type),
        (Boolean, _) => is_numeric(to_type) || to_type == &Utf8,

        (Utf8, Date32) => true,
        (Utf8, Date64) => true,
        (Utf8, _) => is_numeric(to_type),
        (LargeUtf8, _) => is_numeric(to_type),
        (_, Utf8) => is_numeric(from_type) || from_type == &Binary,

        // start numeric casts
        (UInt8, UInt16) => true,
        (UInt8, UInt32) => true,
        (UInt8, UInt64) => true,
        (UInt8, Int8) => true,
        (UInt8, Int16) => true,
        (UInt8, Int32) => true,
        (UInt8, Int64) => true,
        (UInt8, Float32) => true,
        (UInt8, Float64) => true,

        (UInt16, UInt8) => true,
        (UInt16, UInt32) => true,
        (UInt16, UInt64) => true,
        (UInt16, Int8) => true,
        (UInt16, Int16) => true,
        (UInt16, Int32) => true,
        (UInt16, Int64) => true,
        (UInt16, Float32) => true,
        (UInt16, Float64) => true,

        (UInt32, UInt8) => true,
        (UInt32, UInt16) => true,
        (UInt32, UInt64) => true,
        (UInt32, Int8) => true,
        (UInt32, Int16) => true,
        (UInt32, Int32) => true,
        (UInt32, Int64) => true,
        (UInt32, Float32) => true,
        (UInt32, Float64) => true,

        (UInt64, UInt8) => true,
        (UInt64, UInt16) => true,
        (UInt64, UInt32) => true,
        (UInt64, Int8) => true,
        (UInt64, Int16) => true,
        (UInt64, Int32) => true,
        (UInt64, Int64) => true,
        (UInt64, Float32) => true,
        (UInt64, Float64) => true,

        (Int8, UInt8) => true,
        (Int8, UInt16) => true,
        (Int8, UInt32) => true,
        (Int8, UInt64) => true,
        (Int8, Int16) => true,
        (Int8, Int32) => true,
        (Int8, Int64) => true,
        (Int8, Float32) => true,
        (Int8, Float64) => true,

        (Int16, UInt8) => true,
        (Int16, UInt16) => true,
        (Int16, UInt32) => true,
        (Int16, UInt64) => true,
        (Int16, Int8) => true,
        (Int16, Int32) => true,
        (Int16, Int64) => true,
        (Int16, Float32) => true,
        (Int16, Float64) => true,

        (Int32, UInt8) => true,
        (Int32, UInt16) => true,
        (Int32, UInt32) => true,
        (Int32, UInt64) => true,
        (Int32, Int8) => true,
        (Int32, Int16) => true,
        (Int32, Int64) => true,
        (Int32, Float32) => true,
        (Int32, Float64) => true,

        (Int64, UInt8) => true,
        (Int64, UInt16) => true,
        (Int64, UInt32) => true,
        (Int64, UInt64) => true,
        (Int64, Int8) => true,
        (Int64, Int16) => true,
        (Int64, Int32) => true,
        (Int64, Float32) => true,
        (Int64, Float64) => true,

        (Float32, UInt8) => true,
        (Float32, UInt16) => true,
        (Float32, UInt32) => true,
        (Float32, UInt64) => true,
        (Float32, Int8) => true,
        (Float32, Int16) => true,
        (Float32, Int32) => true,
        (Float32, Int64) => true,
        (Float32, Float64) => true,

        (Float64, UInt8) => true,
        (Float64, UInt16) => true,
        (Float64, UInt32) => true,
        (Float64, UInt64) => true,
        (Float64, Int8) => true,
        (Float64, Int16) => true,
        (Float64, Int32) => true,
        (Float64, Int64) => true,
        (Float64, Float32) => true,
        // end numeric casts

        // temporal casts
        (Int32, Date32) => true,
        (Int32, Time32(_)) => true,
        (Date32, Int32) => true,
        (Time32(_), Int32) => true,
        (Int64, Date64) => true,
        (Int64, Time64(_)) => true,
        (Date64, Int64) => true,
        (Time64(_), Int64) => true,
        (Date32, Date64) => true,
        (Date64, Date32) => true,
        (Time32(TimeUnit::Second), Time32(TimeUnit::Millisecond)) => true,
        (Time32(TimeUnit::Millisecond), Time32(TimeUnit::Second)) => true,
        (Time32(_), Time64(_)) => true,
        (Time64(TimeUnit::Microsecond), Time64(TimeUnit::Nanosecond)) => true,
        (Time64(TimeUnit::Nanosecond), Time64(TimeUnit::Microsecond)) => true,
        (Time64(_), Time32(to_unit)) => {
            matches!(to_unit, TimeUnit::Second | TimeUnit::Millisecond)
        }
        (Timestamp(_, _), Int64) => true,
        (Int64, Timestamp(_, _)) => true,
        (Timestamp(_, _), Timestamp(_, _)) => true,
        (Timestamp(_, _), Date32) => true,
        (Timestamp(_, _), Date64) => true,
        // date64 to timestamp might not make sense,
        (Int64, Duration(_)) => true,
        (Null, Int32) => true,
        (_, _) => false,
    }
}

/// Cast `array` to the provided data type and return a new Array with
/// type `to_type`, if possible.
///
/// Behavior:
/// * Boolean to Utf8: `true` => '1', `false` => `0`
/// * Utf8 to numeric: strings that can't be parsed to numbers return null, float strings
///   in integer casts return null
/// * Numeric to boolean: 0 returns `false`, any other value returns `true`
/// * List to List: the underlying data type is cast
/// * Primitive to List: a list array with 1 value per slot is created
/// * Date32 and Date64: precision lost when going to higher interval
/// * Time32 and Time64: precision lost when going to higher interval
/// * Timestamp and Date{32|64}: precision lost when going to higher interval
/// * Temporal to/from backing primitive: zero-copy with data type change
///
/// Unsupported Casts
/// * To or from `StructArray`
/// * List to primitive
/// * Utf8 to boolean
/// * Interval and duration
pub fn cast(array: &dyn Array, to_type: &DataType) -> Result<Box<dyn Array>> {
    use DataType::*;
    let from_type = array.data_type();

    // clone array if types are the same
    if from_type == to_type {
        return Ok(clone(array));
    }
    match (from_type, to_type) {
        (Struct(_), _) => Err(ArrowError::NotYetImplemented(
            "Cannot cast from struct to other types".to_string(),
        )),
        (_, Struct(_)) => Err(ArrowError::NotYetImplemented(
            "Cannot cast to struct from other types".to_string(),
        )),
        (List(_), List(to)) => {
            let array = array.as_any().downcast_ref::<ListArray<i32>>().unwrap();

            let values = array.values();
            let new_values = cast(values.as_ref(), to.data_type())?.into();

            let list = ListArray::<i32>::from_data(
                to_type.clone(),
                array.offsets_buffer().clone(),
                new_values,
                array.validity().clone(),
            );
            Ok(Box::new(list))
        }

        (List(_), _) => Err(ArrowError::NotYetImplemented(
            "Cannot cast list to non-list data types".to_string(),
        )),
        (_, List(to)) => {
            // cast primitive to list's primitive
            let values = cast(array, to.data_type())?.into();
            // create offsets, where if array.len() = 2, we have [0,1,2]
            let offsets =
                unsafe { Buffer::from_trusted_len_iter_unchecked(0..=array.len() as i32) };

            let data_type = ListArray::<i32>::default_datatype(to.data_type().clone());
            let list_array = ListArray::<i32>::from_data(data_type, offsets, values, None);

            Ok(Box::new(list_array))
        }

        (Dictionary(index_type, _), _) => match **index_type {
            DataType::Int8 => dictionary_cast::<i8>(array, to_type),
            DataType::Int16 => dictionary_cast::<i16>(array, to_type),
            DataType::Int32 => dictionary_cast::<i32>(array, to_type),
            DataType::Int64 => dictionary_cast::<i64>(array, to_type),
            DataType::UInt8 => dictionary_cast::<u8>(array, to_type),
            DataType::UInt16 => dictionary_cast::<u16>(array, to_type),
            DataType::UInt32 => dictionary_cast::<u32>(array, to_type),
            DataType::UInt64 => dictionary_cast::<u64>(array, to_type),
            _ => unreachable!(),
        },
        (_, Dictionary(index_type, value_type)) => match **index_type {
            DataType::Int8 => cast_to_dictionary::<i8>(array, value_type),
            DataType::Int16 => cast_to_dictionary::<i16>(array, value_type),
            DataType::Int32 => cast_to_dictionary::<i32>(array, value_type),
            DataType::Int64 => cast_to_dictionary::<i64>(array, value_type),
            DataType::UInt8 => cast_to_dictionary::<u8>(array, value_type),
            DataType::UInt16 => cast_to_dictionary::<u16>(array, value_type),
            DataType::UInt32 => cast_to_dictionary::<u32>(array, value_type),
            DataType::UInt64 => cast_to_dictionary::<u64>(array, value_type),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Casting from type {:?} to dictionary type {:?} not supported",
                from_type, to_type,
            ))),
        },
        (_, Boolean) => match from_type {
            UInt8 => cast_numeric_to_bool::<u8>(array),
            UInt16 => cast_numeric_to_bool::<u16>(array),
            UInt32 => cast_numeric_to_bool::<u32>(array),
            UInt64 => cast_numeric_to_bool::<u64>(array),
            Int8 => cast_numeric_to_bool::<i8>(array),
            Int16 => cast_numeric_to_bool::<i16>(array),
            Int32 => cast_numeric_to_bool::<i32>(array),
            Int64 => cast_numeric_to_bool::<i64>(array),
            Float32 => cast_numeric_to_bool::<f32>(array),
            Float64 => cast_numeric_to_bool::<f64>(array),
            Utf8 => Err(ArrowError::NotYetImplemented(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },
        (Boolean, _) => match to_type {
            UInt8 => cast_bool_to_numeric::<u8>(array, to_type),
            UInt16 => cast_bool_to_numeric::<u16>(array, to_type),
            UInt32 => cast_bool_to_numeric::<u32>(array, to_type),
            UInt64 => cast_bool_to_numeric::<u64>(array, to_type),
            Int8 => cast_bool_to_numeric::<i8>(array, to_type),
            Int16 => cast_bool_to_numeric::<i16>(array, to_type),
            Int32 => cast_bool_to_numeric::<i32>(array, to_type),
            Int64 => cast_bool_to_numeric::<i64>(array, to_type),
            Float32 => cast_bool_to_numeric::<f32>(array, to_type),
            Float64 => cast_bool_to_numeric::<f64>(array, to_type),
            Utf8 => cast_bool_to_utf8::<i32>(array),
            LargeUtf8 => cast_bool_to_utf8::<i64>(array),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },

        (Utf8, _) => match to_type {
            UInt8 => cast_string_to_numeric::<i32, u8>(array, to_type),
            UInt16 => cast_string_to_numeric::<i32, u16>(array, to_type),
            UInt32 => cast_string_to_numeric::<i32, u32>(array, to_type),
            UInt64 => cast_string_to_numeric::<i32, u64>(array, to_type),
            Int8 => cast_string_to_numeric::<i32, i8>(array, to_type),
            Int16 => cast_string_to_numeric::<i32, i16>(array, to_type),
            Int32 => cast_string_to_numeric::<i32, i32>(array, to_type),
            Int64 => cast_string_to_numeric::<i32, i64>(array, to_type),
            Float32 => cast_string_to_numeric::<i32, f32>(array, to_type),
            Float64 => cast_string_to_numeric::<i32, f64>(array, to_type),
            Date32 => Ok(Box::new(to_date32::<i32>(array, to_type))),
            Date64 => Ok(Box::new(to_date64::<i32>(array, to_type))),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },
        (LargeUtf8, _) => match to_type {
            UInt8 => cast_string_to_numeric::<i64, u8>(array, to_type),
            UInt16 => cast_string_to_numeric::<i64, u16>(array, to_type),
            UInt32 => cast_string_to_numeric::<i64, u32>(array, to_type),
            UInt64 => cast_string_to_numeric::<i64, u64>(array, to_type),
            Int8 => cast_string_to_numeric::<i64, i8>(array, to_type),
            Int16 => cast_string_to_numeric::<i64, i16>(array, to_type),
            Int32 => cast_string_to_numeric::<i64, i32>(array, to_type),
            Int64 => cast_string_to_numeric::<i64, i64>(array, to_type),
            Float32 => cast_string_to_numeric::<i64, f32>(array, to_type),
            Float64 => cast_string_to_numeric::<i64, f64>(array, to_type),
            Date32 => Ok(Box::new(to_date32::<i64>(array, to_type))),
            Date64 => Ok(Box::new(to_date64::<i64>(array, to_type))),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },

        (_, Utf8) => match from_type {
            UInt8 => cast_numeric_to_string::<u8, i32>(array),
            UInt16 => cast_numeric_to_string::<u16, i32>(array),
            UInt32 => cast_numeric_to_string::<u32, i32>(array),
            UInt64 => cast_numeric_to_string::<u64, i32>(array),
            Int8 => cast_numeric_to_string::<i8, i32>(array),
            Int16 => cast_numeric_to_string::<i16, i32>(array),
            Int32 => cast_numeric_to_string::<i32, i32>(array),
            Int64 => cast_numeric_to_string::<i64, i32>(array),
            Float32 => cast_numeric_to_string::<f32, i32>(array),
            Float64 => cast_numeric_to_string::<f64, i32>(array),
            Binary => {
                let array = array.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();

                // perf todo: the offsets are equal; we can speed-up this
                let iter = array
                    .iter()
                    .map(|x| x.and_then(|x| std::str::from_utf8(x).ok()));

                let array = Utf8Array::<i32>::from_trusted_len_iter(iter);
                Ok(Box::new(array))
            }
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },

        // start numeric casts
        (UInt8, UInt16) => cast_numeric_arrays::<u8, u16>(array, to_type),
        (UInt8, UInt32) => cast_numeric_arrays::<u8, u32>(array, to_type),
        (UInt8, UInt64) => cast_numeric_arrays::<u8, u64>(array, to_type),
        (UInt8, Int8) => cast_numeric_arrays::<u8, i8>(array, to_type),
        (UInt8, Int16) => cast_numeric_arrays::<u8, i16>(array, to_type),
        (UInt8, Int32) => cast_numeric_arrays::<u8, i32>(array, to_type),
        (UInt8, Int64) => cast_numeric_arrays::<u8, i64>(array, to_type),
        (UInt8, Float32) => cast_numeric_arrays::<u8, f32>(array, to_type),
        (UInt8, Float64) => cast_numeric_arrays::<u8, f64>(array, to_type),

        (UInt16, UInt8) => cast_numeric_arrays::<u16, u8>(array, to_type),
        (UInt16, UInt32) => cast_numeric_arrays::<u16, u32>(array, to_type),
        (UInt16, UInt64) => cast_numeric_arrays::<u16, u64>(array, to_type),
        (UInt16, Int8) => cast_numeric_arrays::<u16, i8>(array, to_type),
        (UInt16, Int16) => cast_numeric_arrays::<u16, i16>(array, to_type),
        (UInt16, Int32) => cast_numeric_arrays::<u16, i32>(array, to_type),
        (UInt16, Int64) => cast_numeric_arrays::<u16, i64>(array, to_type),
        (UInt16, Float32) => cast_numeric_arrays::<u16, f32>(array, to_type),
        (UInt16, Float64) => cast_numeric_arrays::<u16, f64>(array, to_type),

        (UInt32, UInt8) => cast_numeric_arrays::<u32, u8>(array, to_type),
        (UInt32, UInt16) => cast_numeric_arrays::<u32, u16>(array, to_type),
        (UInt32, UInt64) => cast_numeric_arrays::<u32, u64>(array, to_type),
        (UInt32, Int8) => cast_numeric_arrays::<u32, i8>(array, to_type),
        (UInt32, Int16) => cast_numeric_arrays::<u32, i16>(array, to_type),
        (UInt32, Int32) => cast_numeric_arrays::<u32, i32>(array, to_type),
        (UInt32, Int64) => cast_numeric_arrays::<u32, i64>(array, to_type),
        (UInt32, Float32) => cast_numeric_arrays::<u32, f32>(array, to_type),
        (UInt32, Float64) => cast_numeric_arrays::<u32, f64>(array, to_type),

        (UInt64, UInt8) => cast_numeric_arrays::<u64, u8>(array, to_type),
        (UInt64, UInt16) => cast_numeric_arrays::<u64, u16>(array, to_type),
        (UInt64, UInt32) => cast_numeric_arrays::<u64, u32>(array, to_type),
        (UInt64, Int8) => cast_numeric_arrays::<u64, i8>(array, to_type),
        (UInt64, Int16) => cast_numeric_arrays::<u64, i16>(array, to_type),
        (UInt64, Int32) => cast_numeric_arrays::<u64, i32>(array, to_type),
        (UInt64, Int64) => cast_numeric_arrays::<u64, i64>(array, to_type),
        (UInt64, Float32) => cast_numeric_arrays::<u64, f32>(array, to_type),
        (UInt64, Float64) => cast_numeric_arrays::<u64, f64>(array, to_type),

        (Int8, UInt8) => cast_numeric_arrays::<i8, u8>(array, to_type),
        (Int8, UInt16) => cast_numeric_arrays::<i8, u16>(array, to_type),
        (Int8, UInt32) => cast_numeric_arrays::<i8, u32>(array, to_type),
        (Int8, UInt64) => cast_numeric_arrays::<i8, u64>(array, to_type),
        (Int8, Int16) => cast_numeric_arrays::<i8, i16>(array, to_type),
        (Int8, Int32) => cast_numeric_arrays::<i8, i32>(array, to_type),
        (Int8, Int64) => cast_numeric_arrays::<i8, i64>(array, to_type),
        (Int8, Float32) => cast_numeric_arrays::<i8, f32>(array, to_type),
        (Int8, Float64) => cast_numeric_arrays::<i8, f64>(array, to_type),

        (Int16, UInt8) => cast_numeric_arrays::<i16, u8>(array, to_type),
        (Int16, UInt16) => cast_numeric_arrays::<i16, u16>(array, to_type),
        (Int16, UInt32) => cast_numeric_arrays::<i16, u32>(array, to_type),
        (Int16, UInt64) => cast_numeric_arrays::<i16, u64>(array, to_type),
        (Int16, Int8) => cast_numeric_arrays::<i16, i8>(array, to_type),
        (Int16, Int32) => cast_numeric_arrays::<i16, i32>(array, to_type),
        (Int16, Int64) => cast_numeric_arrays::<i16, i64>(array, to_type),
        (Int16, Float32) => cast_numeric_arrays::<i16, f32>(array, to_type),
        (Int16, Float64) => cast_numeric_arrays::<i16, f64>(array, to_type),

        (Int32, UInt8) => cast_numeric_arrays::<i32, u8>(array, to_type),
        (Int32, UInt16) => cast_numeric_arrays::<i32, u16>(array, to_type),
        (Int32, UInt32) => cast_numeric_arrays::<i32, u32>(array, to_type),
        (Int32, UInt64) => cast_numeric_arrays::<i32, u64>(array, to_type),
        (Int32, Int8) => cast_numeric_arrays::<i32, i8>(array, to_type),
        (Int32, Int16) => cast_numeric_arrays::<i32, i16>(array, to_type),
        (Int32, Int64) => cast_numeric_arrays::<i32, i64>(array, to_type),
        (Int32, Float32) => cast_numeric_arrays::<i32, f32>(array, to_type),
        (Int32, Float64) => cast_numeric_arrays::<i32, f64>(array, to_type),

        (Int64, UInt8) => cast_numeric_arrays::<i64, u8>(array, to_type),
        (Int64, UInt16) => cast_numeric_arrays::<i64, u16>(array, to_type),
        (Int64, UInt32) => cast_numeric_arrays::<i64, u32>(array, to_type),
        (Int64, UInt64) => cast_numeric_arrays::<i64, u64>(array, to_type),
        (Int64, Int8) => cast_numeric_arrays::<i64, i8>(array, to_type),
        (Int64, Int16) => cast_numeric_arrays::<i64, i16>(array, to_type),
        (Int64, Int32) => cast_numeric_arrays::<i64, i32>(array, to_type),
        (Int64, Float32) => cast_numeric_arrays::<i64, f32>(array, to_type),
        (Int64, Float64) => cast_numeric_arrays::<i64, f64>(array, to_type),

        (Float32, UInt8) => cast_numeric_arrays::<f32, u8>(array, to_type),
        (Float32, UInt16) => cast_numeric_arrays::<f32, u16>(array, to_type),
        (Float32, UInt32) => cast_numeric_arrays::<f32, u32>(array, to_type),
        (Float32, UInt64) => cast_numeric_arrays::<f32, u64>(array, to_type),
        (Float32, Int8) => cast_numeric_arrays::<f32, i8>(array, to_type),
        (Float32, Int16) => cast_numeric_arrays::<f32, i16>(array, to_type),
        (Float32, Int32) => cast_numeric_arrays::<f32, i32>(array, to_type),
        (Float32, Int64) => cast_numeric_arrays::<f32, i64>(array, to_type),
        (Float32, Float64) => cast_numeric_arrays::<f32, f64>(array, to_type),

        (Float64, UInt8) => cast_numeric_arrays::<f64, u8>(array, to_type),
        (Float64, UInt16) => cast_numeric_arrays::<f64, u16>(array, to_type),
        (Float64, UInt32) => cast_numeric_arrays::<f64, u32>(array, to_type),
        (Float64, UInt64) => cast_numeric_arrays::<f64, u64>(array, to_type),
        (Float64, Int8) => cast_numeric_arrays::<f64, i8>(array, to_type),
        (Float64, Int16) => cast_numeric_arrays::<f64, i16>(array, to_type),
        (Float64, Int32) => cast_numeric_arrays::<f64, i32>(array, to_type),
        (Float64, Int64) => cast_numeric_arrays::<f64, i64>(array, to_type),
        (Float64, Float32) => cast_numeric_arrays::<f64, f32>(array, to_type),
        // end numeric casts

        // temporal casts
        (Int32, Date32) => cast_array_data::<i32>(array, to_type),
        (Int32, Time32(TimeUnit::Second)) => cast_array_data::<i32>(array, to_type),
        (Int32, Time32(TimeUnit::Millisecond)) => cast_array_data::<i32>(array, to_type),
        // No support for microsecond/nanosecond with i32
        (Date32, Int32) => cast_array_data::<i32>(array, to_type),
        (Time32(_), Int32) => cast_array_data::<i32>(array, to_type),
        (Int64, Date64) => cast_array_data::<i64>(array, to_type),
        // No support for second/milliseconds with i64
        (Int64, Time64(TimeUnit::Microsecond)) => cast_array_data::<i64>(array, to_type),
        (Int64, Time64(TimeUnit::Nanosecond)) => cast_array_data::<i64>(array, to_type),

        (Date64, Int64) => cast_array_data::<i64>(array, to_type),
        (Time64(_), Int64) => cast_array_data::<i64>(array, to_type),
        (Date32, Date64) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap();

            let values = unary::<_, _, i64>(array, |x| x as i64 * MILLISECONDS_IN_DAY, to_type);

            Ok(Box::new(values))
        }
        (Date64, Date32) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();

            let values = unary::<_, _, i32>(array, |x| (x / MILLISECONDS_IN_DAY) as i32, to_type);

            Ok(Box::new(values))
        }
        (Time32(TimeUnit::Second), Time32(TimeUnit::Millisecond)) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap();

            let values = unary::<_, _, i32>(array, |x| x * MILLISECONDS as i32, to_type);

            Ok(Box::new(values))
        }
        (Time32(TimeUnit::Millisecond), Time32(TimeUnit::Second)) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap();

            let values = unary::<_, _, i32>(array, |x| x / (MILLISECONDS as i32), to_type);

            Ok(Box::new(values))
        }
        (Time32(from_unit), Time64(to_unit)) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap();

            let from_size = time_unit_multiple(&from_unit);
            let to_size = time_unit_multiple(&to_unit);
            let divisor = to_size / from_size;
            let values = unary::<_, _, i64>(&array, |x| (x as i64 * divisor), to_type);
            Ok(Box::new(values))
        }
        (Time64(TimeUnit::Microsecond), Time64(TimeUnit::Nanosecond)) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();

            let values = unary::<_, _, i64>(array, |x| x * MILLISECONDS, to_type);
            Ok(Box::new(values))
        }
        (Time64(TimeUnit::Nanosecond), Time64(TimeUnit::Microsecond)) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();

            let values = unary::<_, _, i64>(array, |x| x / MILLISECONDS, to_type);
            Ok(Box::new(values))
        }
        (Time64(from_unit), Time32(to_unit)) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();

            let from_size = time_unit_multiple(&from_unit);
            let to_size = time_unit_multiple(&to_unit);
            let divisor = from_size / to_size;
            let values = unary::<_, _, i32>(&array, |x| (x as i64 / divisor) as i32, to_type);
            Ok(Box::new(values))
        }
        (Timestamp(_, _), Int64) => cast_array_data::<i64>(array, &to_type),
        (Int64, Timestamp(_, _)) => cast_array_data::<i64>(array, to_type),
        (Timestamp(from_unit, _), Timestamp(to_unit, _)) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();

            let from_size = time_unit_multiple(&from_unit);
            let to_size = time_unit_multiple(&to_unit);
            // we either divide or multiply, depending on size of each unit
            let array = if from_size >= to_size {
                unary::<_, _, i64>(&array, |x| (x / (from_size / to_size)), to_type)
            } else {
                unary::<_, _, i64>(&array, |x| (x * (to_size / from_size)), to_type)
            };
            Ok(Box::new(array))
        }
        (Timestamp(from_unit, _), Date32) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();

            let from_size = time_unit_multiple(&from_unit) * SECONDS_IN_DAY;
            let array = unary::<_, _, i32>(&array, |x| (x / from_size) as i32, to_type);

            Ok(Box::new(array))
        }
        (Timestamp(from_unit, _), Date64) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();

            let from_size = time_unit_multiple(&from_unit);
            let to_size = MILLISECONDS;

            // Scale time_array by (to_size / from_size) using a
            // single integer operation, but need to avoid integer
            // math rounding down to zero

            match to_size.cmp(&from_size) {
                std::cmp::Ordering::Less => {
                    let array =
                        unary::<_, _, i64>(&array, |x| (x / (from_size / to_size)), to_type);
                    Ok(Box::new(array))
                }
                std::cmp::Ordering::Equal => cast_array_data::<i64>(array, &to_type),
                std::cmp::Ordering::Greater => {
                    let array =
                        unary::<_, _, i64>(&array, |x| (x * (to_size / from_size)), to_type);
                    Ok(Box::new(array))
                }
            }
        }

        // date64 to timestamp might not make sense,
        (Int64, Duration(_)) => cast_array_data::<i64>(array, to_type),

        // null to primitive/flat types
        //(Null, Int32) => Ok(Box::new(Int32Array::from(vec![None; array.len()]))),
        (_, _) => Err(ArrowError::NotYetImplemented(format!(
            "Casting from {:?} to {:?} not supported",
            from_type, to_type,
        ))),
    }
}

/// Get the time unit as a multiple of a second
const fn time_unit_multiple(unit: &TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => MILLISECONDS,
        TimeUnit::Microsecond => MICROSECONDS,
        TimeUnit::Nanosecond => NANOSECONDS,
    }
}

/// Attempts to encode an array into an `ArrayDictionary` with index
/// type K and value (dictionary) type value_type
///
/// K is the key type
fn cast_to_dictionary<K: DictionaryKey>(
    array: &dyn Array,
    dict_value_type: &DataType,
) -> Result<Box<dyn Array>> {
    match *dict_value_type {
        DataType::Int8 => primitive_to_dictionary::<i8, K>(array, dict_value_type),
        DataType::Int16 => primitive_to_dictionary::<i16, K>(array, dict_value_type),
        DataType::Int32 => primitive_to_dictionary::<i32, K>(array, dict_value_type),
        DataType::Int64 => primitive_to_dictionary::<i64, K>(array, dict_value_type),
        DataType::UInt8 => primitive_to_dictionary::<u8, K>(array, dict_value_type),
        DataType::UInt16 => primitive_to_dictionary::<u16, K>(array, dict_value_type),
        DataType::UInt32 => primitive_to_dictionary::<u32, K>(array, dict_value_type),
        DataType::UInt64 => primitive_to_dictionary::<u64, K>(array, dict_value_type),
        DataType::Utf8 => string_to_dictionary::<i32, K>(array),
        DataType::LargeUtf8 => string_to_dictionary::<i64, K>(array),
        _ => Err(ArrowError::NotYetImplemented(format!(
            "Unsupported output type for dictionary packing: {:?}",
            dict_value_type
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cast_i32_to_f64() {
        let array = Primitive::<i32>::from_values(&[5, 6, 7, 8, 9]).to(DataType::Int32);
        let b = cast(&array, &DataType::Float64).unwrap();
        let c = b.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((5.0 - c.value(0)).abs() < f64::EPSILON);
        assert!((6.0 - c.value(1)).abs() < f64::EPSILON);
        assert!((7.0 - c.value(2)).abs() < f64::EPSILON);
        assert!((8.0 - c.value(3)).abs() < f64::EPSILON);
        assert!((9.0 - c.value(4)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_cast_i32_to_u8() {
        let array = Primitive::<i32>::from_values(&[-5, 6, -7, 8, 100000000]).to(DataType::Int32);
        let b = cast(&array, &DataType::UInt8).unwrap();
        let expected =
            Primitive::<u8>::from(&[None, Some(6), None, Some(8), None]).to(DataType::UInt8);
        let c = b.as_any().downcast_ref::<PrimitiveArray<u8>>().unwrap();
        assert_eq!(c, &expected);
    }

    #[test]
    fn test_cast_i32_to_u8_sliced() {
        let array = Primitive::<i32>::from_values(&[-5, 6, -7, 8, 100000000]).to(DataType::Int32);
        let array = array.slice(2, 3);
        let b = cast(&array, &DataType::UInt8).unwrap();
        let expected = Primitive::<u8>::from(&[None, Some(8), None]).to(DataType::UInt8);
        let c = b.as_any().downcast_ref::<PrimitiveArray<u8>>().unwrap();
        assert_eq!(c, &expected);
    }

    #[test]
    fn test_cast_i32_to_i32() {
        let input = &[5, 6, 7, 8, 9];
        let array = Primitive::<i32>::from_values(input).to(DataType::Int32);
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();

        let expected = &[5, 6, 7, 8, 9];
        let expected = Primitive::<i32>::from_values(expected).to(DataType::Int32);
        assert_eq!(c, &expected);
    }

    #[test]
    fn test_cast_i32_to_list_i32() {
        let input = &[5, 6, 7, 8, 9];
        let array = Primitive::<i32>::from_values(input).to(DataType::Int32);
        let b = cast(
            &array,
            &DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
        )
        .unwrap();

        let arr = b.as_any().downcast_ref::<ListArray<i32>>().unwrap();
        assert_eq!(&[0, 1, 2, 3, 4, 5], arr.offsets());
        let values = arr.values();
        let c = values
            .as_any()
            .downcast_ref::<PrimitiveArray<i32>>()
            .unwrap();

        let expected = &[5, 6, 7, 8, 9];
        let expected = Primitive::<i32>::from_values(expected).to(DataType::Int32);
        assert_eq!(c, &expected);
    }

    #[test]
    fn test_cast_i32_to_list_i32_nullable() {
        let input = [Some(5), None, Some(7), Some(8), Some(9)];

        let array = Primitive::<i32>::from(input).to(DataType::Int32);
        let b = cast(
            &array,
            &DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
        )
        .unwrap();

        let arr = b.as_any().downcast_ref::<ListArray<i32>>().unwrap();
        assert_eq!(&[0, 1, 2, 3, 4, 5], arr.offsets());
        let values = arr.values();
        let c = values
            .as_any()
            .downcast_ref::<PrimitiveArray<i32>>()
            .unwrap();

        let expected = &[Some(5), None, Some(7), Some(8), Some(9)];
        let expected = Primitive::<i32>::from(expected).to(DataType::Int32);
        assert_eq!(c, &expected);
    }

    #[test]
    fn test_cast_i32_to_list_f64_nullable_sliced() {
        let input = [Some(5), None, Some(7), Some(8), None, Some(10)];

        let array = Primitive::<i32>::from(input).to(DataType::Int32);

        let array = array.slice(2, 4);
        let b = cast(
            &array,
            &DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
        )
        .unwrap();

        let arr = b.as_any().downcast_ref::<ListArray<i32>>().unwrap();
        assert_eq!(&[0, 1, 2, 3, 4], arr.offsets());
        let values = arr.values();
        let c = values
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();

        let expected = &[Some(7.0), Some(8.0), None, Some(10.0)];
        let expected = Primitive::<f64>::from(expected).to(DataType::Float64);
        assert_eq!(c, &expected);
    }

    #[test]
    fn test_cast_utf8_to_i32() {
        let array = Utf8Array::<i32>::from_slice(&["5", "6", "seven", "8", "9.1"]);
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();

        let expected = &[Some(5), Some(6), None, Some(8), None];
        let expected = Primitive::<i32>::from(expected).to(DataType::Int32);
        assert_eq!(c, &expected);
    }

    #[test]
    fn test_cast_bool_to_i32() {
        let array = BooleanArray::from(vec![Some(true), Some(false), None]);
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();

        let expected = &[Some(1), Some(0), None];
        let expected = Primitive::<i32>::from(expected).to(DataType::Int32);
        assert_eq!(c, &expected);
    }

    #[test]
    fn test_cast_bool_to_f64() {
        let array = BooleanArray::from(vec![Some(true), Some(false), None]);
        let b = cast(&array, &DataType::Float64).unwrap();
        let c = b.as_any().downcast_ref::<Float64Array>().unwrap();

        let expected = &[Some(1.0), Some(0.0), None];
        let expected = Primitive::<f64>::from(expected).to(DataType::Float64);
        assert_eq!(c, &expected);
    }

    #[test]
    #[should_panic(expected = "Casting from Int32 to Timestamp(Microsecond, None) not supported")]
    fn test_cast_int32_to_timestamp() {
        let array = Primitive::<i32>::from(&[Some(2), Some(10), None]).to(DataType::Int32);
        cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();
    }

    /*
    #[test]
    fn test_cast_list_i32_to_list_u16() {
        // Construct a value array
        let value_data = Int32Array::from(vec![0, 0, 0, -1, -2, -1, 2, 100000000]).data();

        let value_offsets = Buffer::from_slice_ref(&[0, 3, 6, 8]);

        // Construct a list array from the above two
        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build();
        let list_array = Arc::new(ListArray::from(list_data)) as ArrayRef;

        let cast_array = cast(
            &list_array,
            &DataType::List(Box::new(Field::new("item", DataType::UInt16, true))),
        )
        .unwrap();
        // 3 negative values should get lost when casting to unsigned,
        // 1 value should overflow
        assert_eq!(4, cast_array.null_count());
        // offsets should be the same
        assert_eq!(
            list_array.data().buffers().to_vec(),
            cast_array.data().buffers().to_vec()
        );
        let array = cast_array
            .as_ref()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(DataType::UInt16, array.value_type());
        assert_eq!(4, array.values().null_count());
        assert_eq!(3, array.value_length(0));
        assert_eq!(3, array.value_length(1));
        assert_eq!(2, array.value_length(2));
        let values = array.values();
        let u16arr = values.as_any().downcast_ref::<UInt16Array>().unwrap();
        assert_eq!(8, u16arr.len());
        assert_eq!(4, u16arr.null_count());

        assert_eq!(0, u16arr.value(0));
        assert_eq!(0, u16arr.value(1));
        assert_eq!(0, u16arr.value(2));
        assert_eq!(false, u16arr.is_valid(3));
        assert_eq!(false, u16arr.is_valid(4));
        assert_eq!(false, u16arr.is_valid(5));
        assert_eq!(2, u16arr.value(6));
        assert_eq!(false, u16arr.is_valid(7));
    }

    #[test]
    #[should_panic(
        expected = "Casting from Int32 to Timestamp(Microsecond, None) not supported"
    )]
    fn test_cast_list_i32_to_list_timestamp() {
        // Construct a value array
        let value_data =
            Int32Array::from(vec![0, 0, 0, -1, -2, -1, 2, 8, 100000000]).data();

        let value_offsets = Buffer::from_slice_ref(&[0, 3, 6, 9]);

        // Construct a list array from the above two
        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build();
        let list_array = Arc::new(ListArray::from(list_data)) as ArrayRef;

        cast(
            &list_array,
            &DataType::List(Box::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ))),
        )
        .unwrap();
    }

    #[test]
    fn test_cast_date32_to_date64() {
        let a = Date32Array::from(vec![10000, 17890]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date64).unwrap();
        let c = b.as_any().downcast_ref::<Date64Array>().unwrap();
        assert_eq!(864000000000, c.value(0));
        assert_eq!(1545696000000, c.value(1));
    }

    #[test]
    fn test_cast_date64_to_date32() {
        let a = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date32).unwrap();
        let c = b.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(10000, c.value(0));
        assert_eq!(17890, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_date32_to_int32() {
        let a = Date32Array::from(vec![10000, 17890]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(10000, c.value(0));
        assert_eq!(17890, c.value(1));
    }

    #[test]
    fn test_cast_int32_to_date32() {
        let a = Int32Array::from(vec![10000, 17890]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date32).unwrap();
        let c = b.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(10000, c.value(0));
        assert_eq!(17890, c.value(1));
    }

    #[test]
    fn test_cast_timestamp_to_date32() {
        let a = TimestampMillisecondArray::from_opt_vec(
            vec![Some(864000000005), Some(1545696000001), None],
            Some(String::from("UTC")),
        );
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date32).unwrap();
        let c = b.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(10000, c.value(0));
        assert_eq!(17890, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_timestamp_to_date64() {
        let a = TimestampMillisecondArray::from_opt_vec(
            vec![Some(864000000005), Some(1545696000001), None],
            None,
        );
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date64).unwrap();
        let c = b.as_any().downcast_ref::<Date64Array>().unwrap();
        assert_eq!(864000000005, c.value(0));
        assert_eq!(1545696000001, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_timestamp_to_i64() {
        let a = TimestampMillisecondArray::from_opt_vec(
            vec![Some(864000000005), Some(1545696000001), None],
            Some("UTC".to_string()),
        );
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Int64).unwrap();
        let c = b.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(&DataType::Int64, c.data_type());
        assert_eq!(864000000005, c.value(0));
        assert_eq!(1545696000001, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_between_timestamps() {
        let a = TimestampMillisecondArray::from_opt_vec(
            vec![Some(864000003005), Some(1545696002001), None],
            None,
        );
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Second, None)).unwrap();
        let c = b.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
        assert_eq!(864000003, c.value(0));
        assert_eq!(1545696002, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_from_f64() {
        let f64_values: Vec<f64> = vec![
            std::i64::MIN as f64,
            std::i32::MIN as f64,
            std::i16::MIN as f64,
            std::i8::MIN as f64,
            0_f64,
            std::u8::MAX as f64,
            std::u16::MAX as f64,
            std::u32::MAX as f64,
            std::u64::MAX as f64,
        ];
        let f64_array: ArrayRef = Arc::new(Float64Array::from(f64_values));

        let f64_expected = vec![
            "-9223372036854776000.0",
            "-2147483648.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "255.0",
            "65535.0",
            "4294967295.0",
            "18446744073709552000.0",
        ];
        assert_eq!(
            f64_expected,
            get_cast_values::<f64>(&f64_array, &DataType::Float64)
        );

        let f32_expected = vec![
            "-9223372000000000000.0",
            "-2147483600.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "255.0",
            "65535.0",
            "4294967300.0",
            "18446744000000000000.0",
        ];
        assert_eq!(
            f32_expected,
            get_cast_values::<f32>(&f64_array, &DataType::Float32)
        );

        let i64_expected = vec![
            "-9223372036854775808",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "255",
            "65535",
            "4294967295",
            "null",
        ];
        assert_eq!(
            i64_expected,
            get_cast_values::<i64>(&f64_array, &DataType::Int64)
        );

        let i32_expected = vec![
            "null",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "255",
            "65535",
            "null",
            "null",
        ];
        assert_eq!(
            i32_expected,
            get_cast_values::<i32>(&f64_array, &DataType::Int32)
        );

        let i16_expected = vec![
            "null", "null", "-32768", "-128", "0", "255", "null", "null", "null",
        ];
        assert_eq!(
            i16_expected,
            get_cast_values::<i16>(&f64_array, &DataType::Int16)
        );

        let i8_expected = vec![
            "null", "null", "null", "-128", "0", "null", "null", "null", "null",
        ];
        assert_eq!(
            i8_expected,
            get_cast_values::<i8>(&f64_array, &DataType::Int8)
        );

        let u64_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "255",
            "65535",
            "4294967295",
            "null",
        ];
        assert_eq!(
            u64_expected,
            get_cast_values::<u64>(&f64_array, &DataType::UInt64)
        );

        let u32_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "255",
            "65535",
            "4294967295",
            "null",
        ];
        assert_eq!(
            u32_expected,
            get_cast_values::<u32>(&f64_array, &DataType::UInt32)
        );

        let u16_expected = vec![
            "null", "null", "null", "null", "0", "255", "65535", "null", "null",
        ];
        assert_eq!(
            u16_expected,
            get_cast_values::<u16>(&f64_array, &DataType::UInt16)
        );

        let u8_expected = vec![
            "null", "null", "null", "null", "0", "255", "null", "null", "null",
        ];
        assert_eq!(
            u8_expected,
            get_cast_values::<u8>(&f64_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_f32() {
        let f32_values: Vec<f32> = vec![
            std::i32::MIN as f32,
            std::i32::MIN as f32,
            std::i16::MIN as f32,
            std::i8::MIN as f32,
            0_f32,
            std::u8::MAX as f32,
            std::u16::MAX as f32,
            std::u32::MAX as f32,
            std::u32::MAX as f32,
        ];
        let f32_array: ArrayRef = Arc::new(Float32Array::from(f32_values));

        let f64_expected = vec![
            "-2147483648.0",
            "-2147483648.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "255.0",
            "65535.0",
            "4294967296.0",
            "4294967296.0",
        ];
        assert_eq!(
            f64_expected,
            get_cast_values::<f64>(&f32_array, &DataType::Float64)
        );

        let f32_expected = vec![
            "-2147483600.0",
            "-2147483600.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "255.0",
            "65535.0",
            "4294967300.0",
            "4294967300.0",
        ];
        assert_eq!(
            f32_expected,
            get_cast_values::<f32>(&f32_array, &DataType::Float32)
        );

        let i64_expected = vec![
            "-2147483648",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "255",
            "65535",
            "4294967296",
            "4294967296",
        ];
        assert_eq!(
            i64_expected,
            get_cast_values::<i64>(&f32_array, &DataType::Int64)
        );

        let i32_expected = vec![
            "-2147483648",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "255",
            "65535",
            "null",
            "null",
        ];
        assert_eq!(
            i32_expected,
            get_cast_values::<i32>(&f32_array, &DataType::Int32)
        );

        let i16_expected = vec![
            "null", "null", "-32768", "-128", "0", "255", "null", "null", "null",
        ];
        assert_eq!(
            i16_expected,
            get_cast_values::<i16>(&f32_array, &DataType::Int16)
        );

        let i8_expected = vec![
            "null", "null", "null", "-128", "0", "null", "null", "null", "null",
        ];
        assert_eq!(
            i8_expected,
            get_cast_values::<i8>(&f32_array, &DataType::Int8)
        );

        let u64_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "255",
            "65535",
            "4294967296",
            "4294967296",
        ];
        assert_eq!(
            u64_expected,
            get_cast_values::<u64>(&f32_array, &DataType::UInt64)
        );

        let u32_expected = vec![
            "null", "null", "null", "null", "0", "255", "65535", "null", "null",
        ];
        assert_eq!(
            u32_expected,
            get_cast_values::<u32>(&f32_array, &DataType::UInt32)
        );

        let u16_expected = vec![
            "null", "null", "null", "null", "0", "255", "65535", "null", "null",
        ];
        assert_eq!(
            u16_expected,
            get_cast_values::<u16>(&f32_array, &DataType::UInt16)
        );

        let u8_expected = vec![
            "null", "null", "null", "null", "0", "255", "null", "null", "null",
        ];
        assert_eq!(
            u8_expected,
            get_cast_values::<u8>(&f32_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_uint64() {
        let u64_values: Vec<u64> = vec![
            0,
            std::u8::MAX as u64,
            std::u16::MAX as u64,
            std::u32::MAX as u64,
            std::u64::MAX,
        ];
        let u64_array: ArrayRef = Arc::new(UInt64Array::from(u64_values));

        let f64_expected = vec![
            "0.0",
            "255.0",
            "65535.0",
            "4294967295.0",
            "18446744073709552000.0",
        ];
        assert_eq!(
            f64_expected,
            get_cast_values::<f64>(&u64_array, &DataType::Float64)
        );

        let f32_expected = vec![
            "0.0",
            "255.0",
            "65535.0",
            "4294967300.0",
            "18446744000000000000.0",
        ];
        assert_eq!(
            f32_expected,
            get_cast_values::<f32>(&u64_array, &DataType::Float32)
        );

        let i64_expected = vec!["0", "255", "65535", "4294967295", "null"];
        assert_eq!(
            i64_expected,
            get_cast_values::<i64>(&u64_array, &DataType::Int64)
        );

        let i32_expected = vec!["0", "255", "65535", "null", "null"];
        assert_eq!(
            i32_expected,
            get_cast_values::<i32>(&u64_array, &DataType::Int32)
        );

        let i16_expected = vec!["0", "255", "null", "null", "null"];
        assert_eq!(
            i16_expected,
            get_cast_values::<i16>(&u64_array, &DataType::Int16)
        );

        let i8_expected = vec!["0", "null", "null", "null", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<i8>(&u64_array, &DataType::Int8)
        );

        let u64_expected =
            vec!["0", "255", "65535", "4294967295", "18446744073709551615"];
        assert_eq!(
            u64_expected,
            get_cast_values::<u64>(&u64_array, &DataType::UInt64)
        );

        let u32_expected = vec!["0", "255", "65535", "4294967295", "null"];
        assert_eq!(
            u32_expected,
            get_cast_values::<u32>(&u64_array, &DataType::UInt32)
        );

        let u16_expected = vec!["0", "255", "65535", "null", "null"];
        assert_eq!(
            u16_expected,
            get_cast_values::<u16>(&u64_array, &DataType::UInt16)
        );

        let u8_expected = vec!["0", "255", "null", "null", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<u8>(&u64_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_uint32() {
        let u32_values: Vec<u32> = vec![
            0,
            std::u8::MAX as u32,
            std::u16::MAX as u32,
            std::u32::MAX as u32,
        ];
        let u32_array: ArrayRef = Arc::new(UInt32Array::from(u32_values));

        let f64_expected = vec!["0.0", "255.0", "65535.0", "4294967295.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<f64>(&u32_array, &DataType::Float64)
        );

        let f32_expected = vec!["0.0", "255.0", "65535.0", "4294967300.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<f32>(&u32_array, &DataType::Float32)
        );

        let i64_expected = vec!["0", "255", "65535", "4294967295"];
        assert_eq!(
            i64_expected,
            get_cast_values::<i64>(&u32_array, &DataType::Int64)
        );

        let i32_expected = vec!["0", "255", "65535", "null"];
        assert_eq!(
            i32_expected,
            get_cast_values::<i32>(&u32_array, &DataType::Int32)
        );

        let i16_expected = vec!["0", "255", "null", "null"];
        assert_eq!(
            i16_expected,
            get_cast_values::<i16>(&u32_array, &DataType::Int16)
        );

        let i8_expected = vec!["0", "null", "null", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<i8>(&u32_array, &DataType::Int8)
        );

        let u64_expected = vec!["0", "255", "65535", "4294967295"];
        assert_eq!(
            u64_expected,
            get_cast_values::<u64>(&u32_array, &DataType::UInt64)
        );

        let u32_expected = vec!["0", "255", "65535", "4294967295"];
        assert_eq!(
            u32_expected,
            get_cast_values::<u32>(&u32_array, &DataType::UInt32)
        );

        let u16_expected = vec!["0", "255", "65535", "null"];
        assert_eq!(
            u16_expected,
            get_cast_values::<u16>(&u32_array, &DataType::UInt16)
        );

        let u8_expected = vec!["0", "255", "null", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<u8>(&u32_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_uint16() {
        let u16_values: Vec<u16> = vec![0, std::u8::MAX as u16, std::u16::MAX as u16];
        let u16_array: ArrayRef = Arc::new(UInt16Array::from(u16_values));

        let f64_expected = vec!["0.0", "255.0", "65535.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<f64>(&u16_array, &DataType::Float64)
        );

        let f32_expected = vec!["0.0", "255.0", "65535.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<f32>(&u16_array, &DataType::Float32)
        );

        let i64_expected = vec!["0", "255", "65535"];
        assert_eq!(
            i64_expected,
            get_cast_values::<i64>(&u16_array, &DataType::Int64)
        );

        let i32_expected = vec!["0", "255", "65535"];
        assert_eq!(
            i32_expected,
            get_cast_values::<i32>(&u16_array, &DataType::Int32)
        );

        let i16_expected = vec!["0", "255", "null"];
        assert_eq!(
            i16_expected,
            get_cast_values::<i16>(&u16_array, &DataType::Int16)
        );

        let i8_expected = vec!["0", "null", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<i8>(&u16_array, &DataType::Int8)
        );

        let u64_expected = vec!["0", "255", "65535"];
        assert_eq!(
            u64_expected,
            get_cast_values::<u64>(&u16_array, &DataType::UInt64)
        );

        let u32_expected = vec!["0", "255", "65535"];
        assert_eq!(
            u32_expected,
            get_cast_values::<u32>(&u16_array, &DataType::UInt32)
        );

        let u16_expected = vec!["0", "255", "65535"];
        assert_eq!(
            u16_expected,
            get_cast_values::<u16>(&u16_array, &DataType::UInt16)
        );

        let u8_expected = vec!["0", "255", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<u8>(&u16_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_uint8() {
        let u8_values: Vec<u8> = vec![0, std::u8::MAX];
        let u8_array: ArrayRef = Arc::new(UInt8Array::from(u8_values));

        let f64_expected = vec!["0.0", "255.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<f64>(&u8_array, &DataType::Float64)
        );

        let f32_expected = vec!["0.0", "255.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<f32>(&u8_array, &DataType::Float32)
        );

        let i64_expected = vec!["0", "255"];
        assert_eq!(
            i64_expected,
            get_cast_values::<i64>(&u8_array, &DataType::Int64)
        );

        let i32_expected = vec!["0", "255"];
        assert_eq!(
            i32_expected,
            get_cast_values::<i32>(&u8_array, &DataType::Int32)
        );

        let i16_expected = vec!["0", "255"];
        assert_eq!(
            i16_expected,
            get_cast_values::<i16>(&u8_array, &DataType::Int16)
        );

        let i8_expected = vec!["0", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<i8>(&u8_array, &DataType::Int8)
        );

        let u64_expected = vec!["0", "255"];
        assert_eq!(
            u64_expected,
            get_cast_values::<u64>(&u8_array, &DataType::UInt64)
        );

        let u32_expected = vec!["0", "255"];
        assert_eq!(
            u32_expected,
            get_cast_values::<u32>(&u8_array, &DataType::UInt32)
        );

        let u16_expected = vec!["0", "255"];
        assert_eq!(
            u16_expected,
            get_cast_values::<u16>(&u8_array, &DataType::UInt16)
        );

        let u8_expected = vec!["0", "255"];
        assert_eq!(
            u8_expected,
            get_cast_values::<u8>(&u8_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_int64() {
        let i64_values: Vec<i64> = vec![
            std::i64::MIN,
            std::i32::MIN as i64,
            std::i16::MIN as i64,
            std::i8::MIN as i64,
            0,
            std::i8::MAX as i64,
            std::i16::MAX as i64,
            std::i32::MAX as i64,
            std::i64::MAX,
        ];
        let i64_array: ArrayRef = Arc::new(Int64Array::from(i64_values));

        let f64_expected = vec![
            "-9223372036854776000.0",
            "-2147483648.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "127.0",
            "32767.0",
            "2147483647.0",
            "9223372036854776000.0",
        ];
        assert_eq!(
            f64_expected,
            get_cast_values::<f64>(&i64_array, &DataType::Float64)
        );

        let f32_expected = vec![
            "-9223372000000000000.0",
            "-2147483600.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "127.0",
            "32767.0",
            "2147483600.0",
            "9223372000000000000.0",
        ];
        assert_eq!(
            f32_expected,
            get_cast_values::<f32>(&i64_array, &DataType::Float32)
        );

        let i64_expected = vec![
            "-9223372036854775808",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "127",
            "32767",
            "2147483647",
            "9223372036854775807",
        ];
        assert_eq!(
            i64_expected,
            get_cast_values::<i64>(&i64_array, &DataType::Int64)
        );

        let i32_expected = vec![
            "null",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "127",
            "32767",
            "2147483647",
            "null",
        ];
        assert_eq!(
            i32_expected,
            get_cast_values::<i32>(&i64_array, &DataType::Int32)
        );

        let i16_expected = vec![
            "null", "null", "-32768", "-128", "0", "127", "32767", "null", "null",
        ];
        assert_eq!(
            i16_expected,
            get_cast_values::<i16>(&i64_array, &DataType::Int16)
        );

        let i8_expected = vec![
            "null", "null", "null", "-128", "0", "127", "null", "null", "null",
        ];
        assert_eq!(
            i8_expected,
            get_cast_values::<i8>(&i64_array, &DataType::Int8)
        );

        let u64_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "127",
            "32767",
            "2147483647",
            "9223372036854775807",
        ];
        assert_eq!(
            u64_expected,
            get_cast_values::<u64>(&i64_array, &DataType::UInt64)
        );

        let u32_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "127",
            "32767",
            "2147483647",
            "null",
        ];
        assert_eq!(
            u32_expected,
            get_cast_values::<u32>(&i64_array, &DataType::UInt32)
        );

        let u16_expected = vec![
            "null", "null", "null", "null", "0", "127", "32767", "null", "null",
        ];
        assert_eq!(
            u16_expected,
            get_cast_values::<u16>(&i64_array, &DataType::UInt16)
        );

        let u8_expected = vec![
            "null", "null", "null", "null", "0", "127", "null", "null", "null",
        ];
        assert_eq!(
            u8_expected,
            get_cast_values::<u8>(&i64_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_int32() {
        let i32_values: Vec<i32> = vec![
            std::i32::MIN as i32,
            std::i16::MIN as i32,
            std::i8::MIN as i32,
            0,
            std::i8::MAX as i32,
            std::i16::MAX as i32,
            std::i32::MAX as i32,
        ];
        let i32_array: ArrayRef = Arc::new(Int32Array::from(i32_values));

        let f64_expected = vec![
            "-2147483648.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "127.0",
            "32767.0",
            "2147483647.0",
        ];
        assert_eq!(
            f64_expected,
            get_cast_values::<f64>(&i32_array, &DataType::Float64)
        );

        let f32_expected = vec![
            "-2147483600.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "127.0",
            "32767.0",
            "2147483600.0",
        ];
        assert_eq!(
            f32_expected,
            get_cast_values::<f32>(&i32_array, &DataType::Float32)
        );

        let i16_expected = vec!["null", "-32768", "-128", "0", "127", "32767", "null"];
        assert_eq!(
            i16_expected,
            get_cast_values::<i16>(&i32_array, &DataType::Int16)
        );

        let i8_expected = vec!["null", "null", "-128", "0", "127", "null", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<i8>(&i32_array, &DataType::Int8)
        );

        let u64_expected =
            vec!["null", "null", "null", "0", "127", "32767", "2147483647"];
        assert_eq!(
            u64_expected,
            get_cast_values::<u64>(&i32_array, &DataType::UInt64)
        );

        let u32_expected =
            vec!["null", "null", "null", "0", "127", "32767", "2147483647"];
        assert_eq!(
            u32_expected,
            get_cast_values::<u32>(&i32_array, &DataType::UInt32)
        );

        let u16_expected = vec!["null", "null", "null", "0", "127", "32767", "null"];
        assert_eq!(
            u16_expected,
            get_cast_values::<u16>(&i32_array, &DataType::UInt16)
        );

        let u8_expected = vec!["null", "null", "null", "0", "127", "null", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<u8>(&i32_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_int16() {
        let i16_values: Vec<i16> = vec![
            std::i16::MIN,
            std::i8::MIN as i16,
            0,
            std::i8::MAX as i16,
            std::i16::MAX,
        ];
        let i16_array: ArrayRef = Arc::new(Int16Array::from(i16_values));

        let f64_expected = vec!["-32768.0", "-128.0", "0.0", "127.0", "32767.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<f64>(&i16_array, &DataType::Float64)
        );

        let f32_expected = vec!["-32768.0", "-128.0", "0.0", "127.0", "32767.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<f32>(&i16_array, &DataType::Float32)
        );

        let i64_expected = vec!["-32768", "-128", "0", "127", "32767"];
        assert_eq!(
            i64_expected,
            get_cast_values::<i64>(&i16_array, &DataType::Int64)
        );

        let i32_expected = vec!["-32768", "-128", "0", "127", "32767"];
        assert_eq!(
            i32_expected,
            get_cast_values::<i32>(&i16_array, &DataType::Int32)
        );

        let i16_expected = vec!["-32768", "-128", "0", "127", "32767"];
        assert_eq!(
            i16_expected,
            get_cast_values::<i16>(&i16_array, &DataType::Int16)
        );

        let i8_expected = vec!["null", "-128", "0", "127", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<i8>(&i16_array, &DataType::Int8)
        );

        let u64_expected = vec!["null", "null", "0", "127", "32767"];
        assert_eq!(
            u64_expected,
            get_cast_values::<u64>(&i16_array, &DataType::UInt64)
        );

        let u32_expected = vec!["null", "null", "0", "127", "32767"];
        assert_eq!(
            u32_expected,
            get_cast_values::<u32>(&i16_array, &DataType::UInt32)
        );

        let u16_expected = vec!["null", "null", "0", "127", "32767"];
        assert_eq!(
            u16_expected,
            get_cast_values::<u16>(&i16_array, &DataType::UInt16)
        );

        let u8_expected = vec!["null", "null", "0", "127", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<u8>(&i16_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_int8() {
        let i8_values: Vec<i8> = vec![std::i8::MIN, 0, std::i8::MAX];
        let i8_array: ArrayRef = Arc::new(Int8Array::from(i8_values));

        let f64_expected = vec!["-128.0", "0.0", "127.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<f64>(&i8_array, &DataType::Float64)
        );

        let f32_expected = vec!["-128.0", "0.0", "127.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<f32>(&i8_array, &DataType::Float32)
        );

        let i64_expected = vec!["-128", "0", "127"];
        assert_eq!(
            i64_expected,
            get_cast_values::<i64>(&i8_array, &DataType::Int64)
        );

        let i32_expected = vec!["-128", "0", "127"];
        assert_eq!(
            i32_expected,
            get_cast_values::<i32>(&i8_array, &DataType::Int32)
        );

        let i16_expected = vec!["-128", "0", "127"];
        assert_eq!(
            i16_expected,
            get_cast_values::<i16>(&i8_array, &DataType::Int16)
        );

        let i8_expected = vec!["-128", "0", "127"];
        assert_eq!(
            i8_expected,
            get_cast_values::<i8>(&i8_array, &DataType::Int8)
        );

        let u64_expected = vec!["null", "0", "127"];
        assert_eq!(
            u64_expected,
            get_cast_values::<u64>(&i8_array, &DataType::UInt64)
        );

        let u32_expected = vec!["null", "0", "127"];
        assert_eq!(
            u32_expected,
            get_cast_values::<u32>(&i8_array, &DataType::UInt32)
        );

        let u16_expected = vec!["null", "0", "127"];
        assert_eq!(
            u16_expected,
            get_cast_values::<u16>(&i8_array, &DataType::UInt16)
        );

        let u8_expected = vec!["null", "0", "127"];
        assert_eq!(
            u8_expected,
            get_cast_values::<u8>(&i8_array, &DataType::UInt8)
        );
    }

    /// Convert `array` into a vector of strings by casting to data type dt
    fn get_cast_values<T>(array: &ArrayRef, dt: &DataType) -> Vec<String>
    where
        T: ArrowNumericType,
    {
        let c = cast(&array, dt).unwrap();
        let a = c.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        let mut v: Vec<String> = vec![];
        for i in 0..array.len() {
            if a.is_null(i) {
                v.push("null".to_string())
            } else {
                v.push(format!("{:?}", a.value(i)));
            }
        }
        v
    }

    #[test]
    fn test_cast_utf8_dict() {
        // FROM a dictionary with of Utf8 values
        use DataType::*;

        let keys_builder = PrimitiveBuilder::<i8>::new(10);
        let values_builder = StringBuilder::new(10);
        let mut builder = StringDictionaryBuilder::new(keys_builder, values_builder);
        builder.append("one").unwrap();
        builder.append_null().unwrap();
        builder.append("three").unwrap();
        let array: ArrayRef = Arc::new(builder.finish());

        let expected = vec!["one", "null", "three"];

        // Test casting TO StringArray
        let cast_type = Utf8;
        let cast_array = cast(&array, &cast_type).expect("cast to UTF-8 failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        // Test casting TO Dictionary (with different index sizes)

        let cast_type = Dictionary(Box::new(Int16), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(Int32), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(Int64), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(UInt8), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(UInt16), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(UInt32), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(UInt64), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);
    }

    #[test]
    fn test_cast_dict_to_dict_bad_index_value_primitive() {
        use DataType::*;
        // test converting from an array that has indexes of a type
        // that are out of bounds for a particular other kind of
        // index.

        let keys_builder = PrimitiveBuilder::<i32>::new(10);
        let values_builder = PrimitiveBuilder::<i64>::new(10);
        let mut builder = PrimitiveDictionaryBuilder::new(keys_builder, values_builder);

        // add 200 distinct values (which can be stored by a
        // dictionary indexed by int32, but not a dictionary indexed
        // with int8)
        for i in 0..200 {
            builder.append(i).unwrap();
        }
        let array: ArrayRef = Arc::new(builder.finish());

        let cast_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let res = cast(&array, &cast_type);
        assert!(res.is_err());
        let actual_error = format!("{:?}", res);
        let expected_error = "Could not convert 72 dictionary indexes from Int32 to Int8";
        assert!(
            actual_error.contains(expected_error),
            "did not find expected error '{}' in actual error '{}'",
            actual_error,
            expected_error
        );
    }

    #[test]
    fn test_cast_dict_to_dict_bad_index_value_utf8() {
        use DataType::*;
        // Same test as test_cast_dict_to_dict_bad_index_value but use
        // string values (and encode the expected behavior here);

        let keys_builder = PrimitiveBuilder::<i32>::new(10);
        let values_builder = StringBuilder::new(10);
        let mut builder = StringDictionaryBuilder::new(keys_builder, values_builder);

        // add 200 distinct values (which can be stored by a
        // dictionary indexed by int32, but not a dictionary indexed
        // with int8)
        for i in 0..200 {
            let val = format!("val{}", i);
            builder.append(&val).unwrap();
        }
        let array: ArrayRef = Arc::new(builder.finish());

        let cast_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let res = cast(&array, &cast_type);
        assert!(res.is_err());
        let actual_error = format!("{:?}", res);
        let expected_error = "Could not convert 72 dictionary indexes from Int32 to Int8";
        assert!(
            actual_error.contains(expected_error),
            "did not find expected error '{}' in actual error '{}'",
            actual_error,
            expected_error
        );
    }

    #[test]
    fn test_cast_primitive_dict() {
        // FROM a dictionary with of INT32 values
        use DataType::*;

        let keys_builder = PrimitiveBuilder::<i8>::new(10);
        let values_builder = PrimitiveBuilder::<i32>::new(10);
        let mut builder = PrimitiveDictionaryBuilder::new(keys_builder, values_builder);
        builder.append(1).unwrap();
        builder.append_null().unwrap();
        builder.append(3).unwrap();
        let array: ArrayRef = Arc::new(builder.finish());

        let expected = vec!["1", "null", "3"];

        // Test casting TO PrimitiveArray, different dictionary type
        let cast_array = cast(&array, &Utf8).expect("cast to UTF-8 failed");
        assert_eq!(array_to_strings(&cast_array), expected);
        assert_eq!(cast_array.data_type(), &Utf8);

        let cast_array = cast(&array, &Int64).expect("cast to int64 failed");
        assert_eq!(array_to_strings(&cast_array), expected);
        assert_eq!(cast_array.data_type(), &Int64);
    }

    #[test]
    fn test_cast_primitive_array_to_dict() {
        use DataType::*;

        let mut builder = PrimitiveBuilder::<i32>::new(10);
        builder.append_value(1).unwrap();
        builder.append_null().unwrap();
        builder.append_value(3).unwrap();
        let array: ArrayRef = Arc::new(builder.finish());

        let expected = vec!["1", "null", "3"];

        // Cast to a dictionary (same value type, Int32)
        let cast_type = Dictionary(Box::new(UInt8), Box::new(Int32));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        // Cast to a dictionary (different value type, Int8)
        let cast_type = Dictionary(Box::new(UInt8), Box::new(Int8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);
    }

    #[test]
    fn test_cast_string_array_to_dict() {
        use DataType::*;

        let array = Arc::new(StringArray::from(vec![Some("one"), None, Some("three")]))
            as ArrayRef;

        let expected = vec!["one", "null", "three"];

        // Cast to a dictionary (same value type, Utf8)
        let cast_type = Dictionary(Box::new(UInt8), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);
    }

    #[test]
    fn test_cast_null_array_to_int32() {
        let array = Arc::new(NullArray::new(6)) as ArrayRef;

        let expected = Int32Array::from(vec![None; 6]);

        // Cast to a dictionary (same value type, Utf8)
        let cast_type = DataType::Int32;
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        let cast_array = as_primitive_array::<i32>(&cast_array);
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(cast_array, &expected);
    }

    /// Print the `DictionaryArray` `array` as a vector of strings
    fn array_to_strings(array: &ArrayRef) -> Vec<String> {
        (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    "null".to_string()
                } else {
                    array_value_to_string(array, i).expect("Convert array to String")
                }
            })
            .collect()
    }

    #[test]
    fn test_cast_utf8_to_date32() {
        use chrono::NaiveDate;
        let from_ymd = chrono::NaiveDate::from_ymd;
        let since = chrono::NaiveDate::signed_duration_since;

        let a = StringArray::from(vec![
            "2000-01-01",          // valid date with leading 0s
            "2000-2-2",            // valid date without leading 0s
            "2000-00-00",          // invalid month and day
            "2000-01-01T12:00:00", // date + time is invalid
            "2000",                // just a year is invalid
        ]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date32).unwrap();
        let c = b.as_any().downcast_ref::<Date32Array>().unwrap();

        // test valid inputs
        let date_value = since(NaiveDate::from_ymd(2000, 1, 1), from_ymd(1970, 1, 1))
            .num_days() as i32;
        assert_eq!(true, c.is_valid(0)); // "2000-01-01"
        assert_eq!(date_value, c.value(0));

        let date_value = since(NaiveDate::from_ymd(2000, 2, 2), from_ymd(1970, 1, 1))
            .num_days() as i32;
        assert_eq!(true, c.is_valid(1)); // "2000-2-2"
        assert_eq!(date_value, c.value(1));

        // test invalid inputs
        assert_eq!(false, c.is_valid(2)); // "2000-00-00"
        assert_eq!(false, c.is_valid(3)); // "2000-01-01T12:00:00"
        assert_eq!(false, c.is_valid(4)); // "2000"
    }

    #[test]
    fn test_cast_utf8_to_date64() {
        let a = StringArray::from(vec![
            "2000-01-01T12:00:00", // date + time valid
            "2020-12-15T12:34:56", // date + time valid
            "2020-2-2T12:34:56",   // valid date time without leading 0s
            "2000-00-00T12:00:00", // invalid month and day
            "2000-01-01 12:00:00", // missing the 'T'
            "2000-01-01",          // just a date is invalid
        ]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date64).unwrap();
        let c = b.as_any().downcast_ref::<Date64Array>().unwrap();

        // test valid inputs
        assert_eq!(true, c.is_valid(0)); // "2000-01-01T12:00:00"
        assert_eq!(946728000000, c.value(0));
        assert_eq!(true, c.is_valid(1)); // "2020-12-15T12:34:56"
        assert_eq!(1608035696000, c.value(1));
        assert_eq!(true, c.is_valid(2)); // "2020-2-2T12:34:56"
        assert_eq!(1580646896000, c.value(2));

        // test invalid inputs
        assert_eq!(false, c.is_valid(3)); // "2000-00-00T12:00:00"
        assert_eq!(false, c.is_valid(4)); // "2000-01-01 12:00:00"
        assert_eq!(false, c.is_valid(5)); // "2000-01-01"
    }

    #[test]
    fn test_can_cast_types() {
        // this function attempts to ensure that can_cast_types stays
        // in sync with cast.  It simply tries all combinations of
        // types and makes sure that if `can_cast_types` returns
        // true, so does `cast`

        let all_types = get_all_types();

        for array in get_arrays_of_all_types() {
            for to_type in &all_types {
                println!("Test casting {:?} --> {:?}", array.data_type(), to_type);
                let cast_result = cast(&array, &to_type);
                let reported_cast_ability = can_cast_types(array.data_type(), to_type);

                // check for mismatch
                match (cast_result, reported_cast_ability) {
                    (Ok(_), false) => {
                        panic!("Was able to cast array from {:?} to {:?} but can_cast_types reported false",
                               array.data_type(), to_type)
                    }
                    (Err(e), true) => {
                        panic!("Was not able to cast array from {:?} to {:?} but can_cast_types reported true. \
                                Error was {:?}",
                               array.data_type(), to_type, e)
                    }
                    // otherwise it was a match
                    _ => {}
                };
            }
        }
    }

    /// Create instances of arrays with varying types for cast tests
    fn get_arrays_of_all_types() -> Vec<ArrayRef> {
        let tz_name = String::from("America/New_York");
        let binary_data: Vec<&[u8]> = vec![b"foo", b"bar"];
        vec![
            Arc::new(BinaryArray::from(binary_data.clone())),
            Arc::new(LargeBinaryArray::from(binary_data.clone())),
            make_dictionary_primitive::<i8>(),
            make_dictionary_primitive::<i16>(),
            make_dictionary_primitive::<i32>(),
            make_dictionary_primitive::<i64>(),
            make_dictionary_primitive::<u8>(),
            make_dictionary_primitive::<u16>(),
            make_dictionary_primitive::<u32>(),
            make_dictionary_primitive::<u64>(),
            make_dictionary_utf8::<i8>(),
            make_dictionary_utf8::<i16>(),
            make_dictionary_utf8::<i32>(),
            make_dictionary_utf8::<i64>(),
            make_dictionary_utf8::<u8>(),
            make_dictionary_utf8::<u16>(),
            make_dictionary_utf8::<u32>(),
            make_dictionary_utf8::<u64>(),
            Arc::new(make_list_array()),
            Arc::new(make_large_list_array()),
            Arc::new(make_fixed_size_list_array()),
            Arc::new(make_fixed_size_binary_array()),
            Arc::new(StructArray::from(vec![
                (
                    Field::new("a", DataType::Boolean, false),
                    Arc::new(BooleanArray::from(vec![false, false, true, true]))
                        as Arc<Array>,
                ),
                (
                    Field::new("b", DataType::Int32, false),
                    Arc::new(Int32Array::from(vec![42, 28, 19, 31])),
                ),
            ])),
            //Arc::new(make_union_array()),
            Arc::new(NullArray::new(10)),
            Arc::new(StringArray::from(vec!["foo", "bar"])),
            Arc::new(LargeStringArray::from(vec!["foo", "bar"])),
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(Int8Array::from(vec![1, 2])),
            Arc::new(Int16Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(UInt8Array::from(vec![1, 2])),
            Arc::new(UInt16Array::from(vec![1, 2])),
            Arc::new(UInt32Array::from(vec![1, 2])),
            Arc::new(UInt64Array::from(vec![1, 2])),
            Arc::new(Float32Array::from(vec![1.0, 2.0])),
            Arc::new(Float64Array::from(vec![1.0, 2.0])),
            Arc::new(TimestampSecondArray::from_vec(vec![1000, 2000], None)),
            Arc::new(TimestampMillisecondArray::from_vec(vec![1000, 2000], None)),
            Arc::new(TimestampMicrosecondArray::from_vec(vec![1000, 2000], None)),
            Arc::new(TimestampNanosecondArray::from_vec(vec![1000, 2000], None)),
            Arc::new(TimestampSecondArray::from_vec(
                vec![1000, 2000],
                Some(tz_name.clone()),
            )),
            Arc::new(TimestampMillisecondArray::from_vec(
                vec![1000, 2000],
                Some(tz_name.clone()),
            )),
            Arc::new(TimestampMicrosecondArray::from_vec(
                vec![1000, 2000],
                Some(tz_name.clone()),
            )),
            Arc::new(TimestampNanosecondArray::from_vec(
                vec![1000, 2000],
                Some(tz_name),
            )),
            Arc::new(Date32Array::from(vec![1000, 2000])),
            Arc::new(Date64Array::from(vec![1000, 2000])),
            Arc::new(Time32SecondArray::from(vec![1000, 2000])),
            Arc::new(Time32MillisecondArray::from(vec![1000, 2000])),
            Arc::new(Time64MicrosecondArray::from(vec![1000, 2000])),
            Arc::new(Time64NanosecondArray::from(vec![1000, 2000])),
            Arc::new(IntervalYearMonthArray::from(vec![1000, 2000])),
            Arc::new(IntervalDayTimeArray::from(vec![1000, 2000])),
            Arc::new(DurationSecondArray::from(vec![1000, 2000])),
            Arc::new(DurationMillisecondArray::from(vec![1000, 2000])),
            Arc::new(DurationMicrosecondArray::from(vec![1000, 2000])),
            Arc::new(DurationNanosecondArray::from(vec![1000, 2000])),
        ]
    }

    fn make_list_array() -> ListArray {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7]))
            .build();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let value_offsets = Buffer::from_slice_ref(&[0, 3, 6, 8]);

        // Construct a list array from the above two
        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build();
        ListArray::from(list_data)
    }

    fn make_large_list_array() -> LargeListArray {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7]))
            .build();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let value_offsets = Buffer::from_slice_ref(&[0i64, 3, 6, 8]);

        // Construct a list array from the above two
        let list_data_type =
            DataType::LargeList(Box::new(Field::new("item", DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build();
        LargeListArray::from(list_data)
    }

    fn make_fixed_size_list_array() -> FixedSizeListArray {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .build();

        // Construct a fixed size list array from the above two
        let list_data_type = DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::Int32, true)),
            2,
        );
        let list_data = ArrayData::builder(list_data_type)
            .len(5)
            .add_child_data(value_data)
            .build();
        FixedSizeListArray::from(list_data)
    }

    fn make_fixed_size_binary_array() -> FixedSizeBinaryArray {
        let values: [u8; 15] = *b"hellotherearrow";

        let array_data = ArrayData::builder(DataType::FixedSizeBinary(5))
            .len(3)
            .add_buffer(Buffer::from(&values[..]))
            .build();
        FixedSizeBinaryArray::from(array_data)
    }

    fn make_union_array() -> UnionArray {
        let mut builder = UnionBuilder::new_dense(7);
        builder.append::<i32>("a", 1).unwrap();
        builder.append::<i64>("b", 2).unwrap();
        builder.build().unwrap()
    }

    /// Creates a dictionary with primitive dictionary values, and keys of type K
    fn make_dictionary_primitive<K: ArrowDictionaryKeyType>() -> ArrayRef {
        let keys_builder = PrimitiveBuilder::<K>::new(2);
        // Pick Int32 arbitrarily for dictionary values
        let values_builder = PrimitiveBuilder::<i32>::new(2);
        let mut b = PrimitiveDictionaryBuilder::new(keys_builder, values_builder);
        b.append(1).unwrap();
        b.append(2).unwrap();
        Arc::new(b.finish())
    }

    /// Creates a dictionary with utf8 values, and keys of type K
    fn make_dictionary_utf8<K: ArrowDictionaryKeyType>() -> ArrayRef {
        let keys_builder = PrimitiveBuilder::<K>::new(2);
        // Pick Int32 arbitrarily for dictionary values
        let values_builder = StringBuilder::new(2);
        let mut b = StringDictionaryBuilder::new(keys_builder, values_builder);
        b.append("foo").unwrap();
        b.append("bar").unwrap();
        Arc::new(b.finish())
    }

    // Get a selection of datatypes to try and cast to
    fn get_all_types() -> Vec<DataType> {
        use DataType::*;
        let tz_name = String::from("America/New_York");

        vec![
            Null,
            Boolean,
            Int8,
            Int16,
            Int32,
            UInt64,
            UInt8,
            UInt16,
            UInt32,
            UInt64,
            Float16,
            Float32,
            Float64,
            Timestamp(TimeUnit::Second, None),
            Timestamp(TimeUnit::Millisecond, None),
            Timestamp(TimeUnit::Microsecond, None),
            Timestamp(TimeUnit::Nanosecond, None),
            Timestamp(TimeUnit::Second, Some(tz_name.clone())),
            Timestamp(TimeUnit::Millisecond, Some(tz_name.clone())),
            Timestamp(TimeUnit::Microsecond, Some(tz_name.clone())),
            Timestamp(TimeUnit::Nanosecond, Some(tz_name)),
            Date32,
            Date64,
            Time32(TimeUnit::Second),
            Time32(TimeUnit::Millisecond),
            Time64(TimeUnit::Microsecond),
            Time64(TimeUnit::Nanosecond),
            Duration(TimeUnit::Second),
            Duration(TimeUnit::Millisecond),
            Duration(TimeUnit::Microsecond),
            Duration(TimeUnit::Nanosecond),
            Interval(IntervalUnit::YearMonth),
            Interval(IntervalUnit::DayTime),
            Binary,
            FixedSizeBinary(10),
            LargeBinary,
            Utf8,
            LargeUtf8,
            List(Box::new(Field::new("item", DataType::Int8, true))),
            List(Box::new(Field::new("item", DataType::Utf8, true))),
            FixedSizeList(Box::new(Field::new("item", DataType::Int8, true)), 10),
            FixedSizeList(Box::new(Field::new("item", DataType::Utf8, false)), 10),
            LargeList(Box::new(Field::new("item", DataType::Int8, true))),
            LargeList(Box::new(Field::new("item", DataType::Utf8, false))),
            Struct(vec![
                Field::new("f1", DataType::Int32, false),
                Field::new("f2", DataType::Utf8, true),
            ]),
            Union(vec![
                Field::new("f1", DataType::Int32, false),
                Field::new("f2", DataType::Utf8, true),
            ]),
            Dictionary(Box::new(DataType::Int8), Box::new(DataType::Int32)),
            Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
        ]
    }
    */
}
