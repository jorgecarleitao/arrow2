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
    datatypes::*,
    error::{ArrowError, Result},
};

mod binary_to;
mod boolean_to;
mod dictionary_to;
mod primitive_to;
mod timestamps;
mod utf8_to;

pub use binary_to::*;
pub use boolean_to::*;
pub use dictionary_to::*;
pub use primitive_to::*;
pub use timestamps::*;
pub use utf8_to::*;

/// options defining how Cast kernels behave
#[derive(Clone, Copy, Debug)]
pub struct CastOptions {
    /// default to false
    /// whether an overflowing cast should be converted to `None` (default), or be wrapped (i.e. `256i16 as u8 = 0` vectorized).
    /// Settings this to `true` is 5-6x faster for numeric types.
    pub wrapped: bool,
}

impl Default for CastOptions {
    fn default() -> Self {
        Self { wrapped: false }
    }
}

impl CastOptions {
    fn with_wrapped(&self, v: bool) -> Self {
        let mut option = self.clone();
        option.wrapped = v;
        option
    }
}

/// Returns true if this type is numeric: (UInt*, Unit*, or Float*).
fn is_numeric(t: &DataType) -> bool {
    use DataType::*;
    matches!(
        t,
        UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64 | Float32 | Float64
    )
}

macro_rules! primitive_dyn {
    ($from:expr, $expr:tt) => {{
        let from = $from.as_any().downcast_ref().unwrap();
        Ok(Box::new($expr(from)))
    }};
    ($from:expr, $expr:tt, $to:expr) => {{
        let from = $from.as_any().downcast_ref().unwrap();
        Ok(Box::new($expr(from, $to)))
    }};
    ($from:expr, $expr:tt, $from_t:expr, $to:expr) => {{
        let from = $from.as_any().downcast_ref().unwrap();
        Ok(Box::new($expr(from, $from_t, $to)))
    }};
    ($from:expr, $expr:tt, $arg1:expr, $arg2:expr, $arg3:expr) => {{
        let from = $from.as_any().downcast_ref().unwrap();
        Ok(Box::new($expr(from, $arg1, $arg2, $arg3)))
    }};
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
        (LargeList(list_from), LargeList(list_to)) => {
            can_cast_types(list_from.data_type(), list_to.data_type())
        }
        (List(list_from), LargeList(list_to)) if list_from == list_to => true,
        (LargeList(list_from), List(list_to)) if list_from == list_to => true,
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
        (Utf8, Timestamp(TimeUnit::Nanosecond, None)) => true,
        (Utf8, LargeUtf8) => true,
        (Utf8, _) => is_numeric(to_type),
        (LargeUtf8, Timestamp(TimeUnit::Nanosecond, None)) => true,
        (LargeUtf8, Utf8) => true,
        (LargeUtf8, _) => is_numeric(to_type),
        (_, Utf8) => is_numeric(from_type) || from_type == &Binary,
        (_, LargeUtf8) => is_numeric(from_type) || from_type == &Binary,

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
        (Int64, Duration(_)) => true,
        (Duration(_), Int64) => true,
        (Null, Int32) => true,
        (_, _) => false,
    }
}

fn cast_list<O: Offset>(
    array: &ListArray<O>,
    to_type: &DataType,
    options: CastOptions,
) -> Result<ListArray<O>> {
    let values = array.values();
    let new_values = cast_with_options(
        values.as_ref(),
        ListArray::<O>::get_child_type(to_type),
        options,
    )?
    .into();

    Ok(ListArray::<O>::from_data(
        to_type.clone(),
        array.offsets().clone(),
        new_values,
        array.validity().clone(),
    ))
}

fn cast_list_to_large_list(array: &ListArray<i32>, to_type: &DataType) -> ListArray<i64> {
    let offsets = array.offsets();
    let offsets = offsets.iter().map(|x| *x as i64);
    let offets = Buffer::from_trusted_len_iter(offsets);

    ListArray::<i64>::from_data(
        to_type.clone(),
        offets,
        array.values().clone(),
        array.validity().clone(),
    )
}

fn cast_large_to_list(array: &ListArray<i64>, to_type: &DataType) -> ListArray<i32> {
    let offsets = array.offsets();
    let offsets = offsets.iter().map(|x| *x as i32);
    let offets = Buffer::from_trusted_len_iter(offsets);

    ListArray::<i32>::from_data(
        to_type.clone(),
        offets,
        array.values().clone(),
        array.validity().clone(),
    )
}

/// Cast `array` to the provided data type and return a new [`Array`] with
/// type `to_type`, if possible.
///
/// Behavior:
/// * PrimitiveArray to PrimitiveArray: overflowing cast will be None
/// * Boolean to Utf8: `true` => '1', `false` => `0`
/// * Utf8 to numeric: strings that can't be parsed to numbers return null, float strings
///   in integer casts return null
/// * Numeric to boolean: 0 returns `false`, any other value returns `true`
/// * List to List: the underlying data type is cast
/// * PrimitiveArray to List: a list array with 1 value per slot is created
/// * Date32 and Date64: precision lost when going to higher interval
/// * Time32 and Time64: precision lost when going to higher interval
/// * Timestamp and Date{32|64}: precision lost when going to higher interval
/// * Temporal to/from backing primitive: zero-copy with data type change
/// Unsupported Casts
/// * To or from `StructArray`
/// * List to primitive
/// * Utf8 to boolean
/// * Interval and duration
pub fn cast(array: &dyn Array, to_type: &DataType) -> Result<Box<dyn Array>> {
    cast_with_options(array, to_type, CastOptions { wrapped: false })
}

/// Similar to [`cast`], but overflowing cast is wrapped
/// Behavior:
/// * PrimitiveArray to PrimitiveArray: overflowing cast will be wrapped (i.e. `256i16 as u8 = 0` vectorized).
pub fn wrapped_cast(array: &dyn Array, to_type: &DataType) -> Result<Box<dyn Array>> {
    cast_with_options(array, to_type, CastOptions { wrapped: true })
}

#[inline]
pub fn cast_with_options(
    array: &dyn Array,
    to_type: &DataType,
    options: CastOptions,
) -> Result<Box<dyn Array>> {
    use DataType::*;
    let from_type = array.data_type();

    // clone array if types are the same
    if from_type == to_type {
        return Ok(clone(array));
    }

    let as_options = options.with_wrapped(true);
    match (from_type, to_type) {
        (Struct(_), _) => Err(ArrowError::NotYetImplemented(
            "Cannot cast from struct to other types".to_string(),
        )),
        (_, Struct(_)) => Err(ArrowError::NotYetImplemented(
            "Cannot cast to struct from other types".to_string(),
        )),
        (List(_), List(_)) => {
            cast_list::<i32>(array.as_any().downcast_ref().unwrap(), to_type, options)
                .map(|x| Box::new(x) as Box<dyn Array>)
        }
        (LargeList(_), LargeList(_)) => {
            cast_list::<i64>(array.as_any().downcast_ref().unwrap(), to_type, options)
                .map(|x| Box::new(x) as Box<dyn Array>)
        }
        (List(lhs), LargeList(rhs)) if lhs == rhs => Ok(cast_list_to_large_list(
            array.as_any().downcast_ref().unwrap(),
            to_type,
        ))
        .map(|x| Box::new(x) as Box<dyn Array>),
        (List(lhs), LargeList(rhs)) if lhs == rhs => Ok(cast_large_to_list(
            array.as_any().downcast_ref().unwrap(),
            to_type,
        ))
        .map(|x| Box::new(x) as Box<dyn Array>),

        (_, List(to)) => {
            // cast primitive to list's primitive
            let values = cast_with_options(array, to.data_type(), options)?.into();
            // create offsets, where if array.len() = 2, we have [0,1,2]
            let offsets =
                unsafe { Buffer::from_trusted_len_iter_unchecked(0..=array.len() as i32) };

            let data_type = ListArray::<i32>::default_datatype(to.data_type().clone());
            let list_array = ListArray::<i32>::from_data(data_type, offsets, values, None);

            Ok(Box::new(list_array))
        }

        (Dictionary(index_type, _), _) => match **index_type {
            DataType::Int8 => dictionary_cast_dyn::<i8>(array, to_type, options),
            DataType::Int16 => dictionary_cast_dyn::<i16>(array, to_type, options),
            DataType::Int32 => dictionary_cast_dyn::<i32>(array, to_type, options),
            DataType::Int64 => dictionary_cast_dyn::<i64>(array, to_type, options),
            DataType::UInt8 => dictionary_cast_dyn::<u8>(array, to_type, options),
            DataType::UInt16 => dictionary_cast_dyn::<u16>(array, to_type, options),
            DataType::UInt32 => dictionary_cast_dyn::<u32>(array, to_type, options),
            DataType::UInt64 => dictionary_cast_dyn::<u64>(array, to_type, options),
            _ => unreachable!(),
        },
        (_, Dictionary(index_type, value_type)) => match **index_type {
            DataType::Int8 => cast_to_dictionary::<i8>(array, value_type, options),
            DataType::Int16 => cast_to_dictionary::<i16>(array, value_type, options),
            DataType::Int32 => cast_to_dictionary::<i32>(array, value_type, options),
            DataType::Int64 => cast_to_dictionary::<i64>(array, value_type, options),
            DataType::UInt8 => cast_to_dictionary::<u8>(array, value_type, options),
            DataType::UInt16 => cast_to_dictionary::<u16>(array, value_type, options),
            DataType::UInt32 => cast_to_dictionary::<u32>(array, value_type, options),
            DataType::UInt64 => cast_to_dictionary::<u64>(array, value_type, options),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Casting from type {:?} to dictionary type {:?} not supported",
                from_type, to_type,
            ))),
        },
        (_, Boolean) => match from_type {
            UInt8 => primitive_to_boolean_dyn::<u8>(array),
            UInt16 => primitive_to_boolean_dyn::<u16>(array),
            UInt32 => primitive_to_boolean_dyn::<u32>(array),
            UInt64 => primitive_to_boolean_dyn::<u64>(array),
            Int8 => primitive_to_boolean_dyn::<i8>(array),
            Int16 => primitive_to_boolean_dyn::<i16>(array),
            Int32 => primitive_to_boolean_dyn::<i32>(array),
            Int64 => primitive_to_boolean_dyn::<i64>(array),
            Float32 => primitive_to_boolean_dyn::<f32>(array),
            Float64 => primitive_to_boolean_dyn::<f64>(array),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },
        (Boolean, _) => match to_type {
            UInt8 => boolean_to_primitive_dyn::<u8>(array),
            UInt16 => boolean_to_primitive_dyn::<u16>(array),
            UInt32 => boolean_to_primitive_dyn::<u32>(array),
            UInt64 => boolean_to_primitive_dyn::<u64>(array),
            Int8 => boolean_to_primitive_dyn::<i8>(array),
            Int16 => boolean_to_primitive_dyn::<i16>(array),
            Int32 => boolean_to_primitive_dyn::<i32>(array),
            Int64 => boolean_to_primitive_dyn::<i64>(array),
            Float32 => boolean_to_primitive_dyn::<f32>(array),
            Float64 => boolean_to_primitive_dyn::<f64>(array),
            Utf8 => boolean_to_utf8_dyn::<i32>(array),
            LargeUtf8 => boolean_to_utf8_dyn::<i64>(array),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },

        (Utf8, _) => match to_type {
            UInt8 => utf8_to_primitive_dyn::<i32, u8>(array, to_type),
            UInt16 => utf8_to_primitive_dyn::<i32, u16>(array, to_type),
            UInt32 => utf8_to_primitive_dyn::<i32, u32>(array, to_type),
            UInt64 => utf8_to_primitive_dyn::<i32, u64>(array, to_type),
            Int8 => utf8_to_primitive_dyn::<i32, i8>(array, to_type),
            Int16 => utf8_to_primitive_dyn::<i32, i16>(array, to_type),
            Int32 => utf8_to_primitive_dyn::<i32, i32>(array, to_type),
            Int64 => utf8_to_primitive_dyn::<i32, i64>(array, to_type),
            Float32 => utf8_to_primitive_dyn::<i32, f32>(array, to_type),
            Float64 => utf8_to_primitive_dyn::<i32, f64>(array, to_type),
            Date32 => utf8_to_date32_dyn::<i32>(array),
            Date64 => utf8_to_date64_dyn::<i32>(array),
            LargeUtf8 => Ok(Box::new(utf8_to_large_utf8(
                array.as_any().downcast_ref().unwrap(),
            ))),
            Timestamp(TimeUnit::Nanosecond, None) => utf8_to_timestamp_ns_dyn::<i32>(array),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },
        (LargeUtf8, _) => match to_type {
            UInt8 => utf8_to_primitive_dyn::<i64, u8>(array, to_type),
            UInt16 => utf8_to_primitive_dyn::<i64, u16>(array, to_type),
            UInt32 => utf8_to_primitive_dyn::<i64, u32>(array, to_type),
            UInt64 => utf8_to_primitive_dyn::<i64, u64>(array, to_type),
            Int8 => utf8_to_primitive_dyn::<i64, i8>(array, to_type),
            Int16 => utf8_to_primitive_dyn::<i64, i16>(array, to_type),
            Int32 => utf8_to_primitive_dyn::<i64, i32>(array, to_type),
            Int64 => utf8_to_primitive_dyn::<i64, i64>(array, to_type),
            Float32 => utf8_to_primitive_dyn::<i64, f32>(array, to_type),
            Float64 => utf8_to_primitive_dyn::<i64, f64>(array, to_type),
            Date32 => utf8_to_date32_dyn::<i64>(array),
            Date64 => utf8_to_date64_dyn::<i64>(array),
            Utf8 => utf8_large_to_utf8(array.as_any().downcast_ref().unwrap())
                .map(|x| Box::new(x) as Box<dyn Array>),
            Timestamp(TimeUnit::Nanosecond, None) => utf8_to_timestamp_ns_dyn::<i64>(array),
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },

        (_, Utf8) => match from_type {
            UInt8 => primitive_to_utf8_dyn::<u8, i32>(array),
            UInt16 => primitive_to_utf8_dyn::<u16, i32>(array),
            UInt32 => primitive_to_utf8_dyn::<u32, i32>(array),
            UInt64 => primitive_to_utf8_dyn::<u64, i32>(array),
            Int8 => primitive_to_utf8_dyn::<i8, i32>(array),
            Int16 => primitive_to_utf8_dyn::<i16, i32>(array),
            Int32 => primitive_to_utf8_dyn::<i32, i32>(array),
            Int64 => primitive_to_utf8_dyn::<i64, i32>(array),
            Float32 => primitive_to_utf8_dyn::<f32, i32>(array),
            Float64 => primitive_to_utf8_dyn::<f64, i32>(array),
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

        (_, LargeUtf8) => match from_type {
            UInt8 => primitive_to_utf8_dyn::<u8, i64>(array),
            UInt16 => primitive_to_utf8_dyn::<u16, i64>(array),
            UInt32 => primitive_to_utf8_dyn::<u32, i64>(array),
            UInt64 => primitive_to_utf8_dyn::<u64, i64>(array),
            Int8 => primitive_to_utf8_dyn::<i8, i64>(array),
            Int16 => primitive_to_utf8_dyn::<i16, i64>(array),
            Int32 => primitive_to_utf8_dyn::<i32, i64>(array),
            Int64 => primitive_to_utf8_dyn::<i64, i64>(array),
            Float32 => primitive_to_utf8_dyn::<f32, i64>(array),
            Float64 => primitive_to_utf8_dyn::<f64, i64>(array),
            Binary => {
                let array = array.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();

                // perf todo: the offsets are equal; we can speed-up this
                let iter = array
                    .iter()
                    .map(|x| x.and_then(|x| std::str::from_utf8(x).ok()));

                let array = Utf8Array::<i64>::from_trusted_len_iter(iter);
                Ok(Box::new(array))
            }
            _ => Err(ArrowError::NotYetImplemented(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },

        (Binary, LargeBinary) => Ok(Box::new(binary_to_large_binary(
            array.as_any().downcast_ref().unwrap(),
        ))),
        (LargeBinary, Binary) => binary_large_to_binary(array.as_any().downcast_ref().unwrap())
            .map(|x| Box::new(x) as Box<dyn Array>),

        // start numeric casts
        (UInt8, UInt16) => primitive_to_primitive_dyn::<u8, u16>(array, to_type, as_options),
        (UInt8, UInt32) => primitive_to_primitive_dyn::<u8, u32>(array, to_type, as_options),
        (UInt8, UInt64) => primitive_to_primitive_dyn::<u8, u64>(array, to_type, as_options),
        (UInt8, Int8) => primitive_to_primitive_dyn::<u8, i8>(array, to_type, options),
        (UInt8, Int16) => primitive_to_primitive_dyn::<u8, i16>(array, to_type, options),
        (UInt8, Int32) => primitive_to_primitive_dyn::<u8, i32>(array, to_type, options),
        (UInt8, Int64) => primitive_to_primitive_dyn::<u8, i64>(array, to_type, options),
        (UInt8, Float32) => primitive_to_primitive_dyn::<u8, f32>(array, to_type, as_options),
        (UInt8, Float64) => primitive_to_primitive_dyn::<u8, f64>(array, to_type, as_options),

        (UInt16, UInt8) => primitive_to_primitive_dyn::<u16, u8>(array, to_type, options),
        (UInt16, UInt32) => primitive_to_primitive_dyn::<u16, u32>(array, to_type, as_options),
        (UInt16, UInt64) => primitive_to_primitive_dyn::<u16, u64>(array, to_type, as_options),
        (UInt16, Int8) => primitive_to_primitive_dyn::<u16, i8>(array, to_type, options),
        (UInt16, Int16) => primitive_to_primitive_dyn::<u16, i16>(array, to_type, options),
        (UInt16, Int32) => primitive_to_primitive_dyn::<u16, i32>(array, to_type, options),
        (UInt16, Int64) => primitive_to_primitive_dyn::<u16, i64>(array, to_type, options),
        (UInt16, Float32) => primitive_to_primitive_dyn::<u16, f32>(array, to_type, as_options),
        (UInt16, Float64) => primitive_to_primitive_dyn::<u16, f64>(array, to_type, as_options),

        (UInt32, UInt8) => primitive_to_primitive_dyn::<u32, u8>(array, to_type, options),
        (UInt32, UInt16) => primitive_to_primitive_dyn::<u32, u16>(array, to_type, options),
        (UInt32, UInt64) => primitive_to_primitive_dyn::<u32, u64>(array, to_type, as_options),
        (UInt32, Int8) => primitive_to_primitive_dyn::<u32, i8>(array, to_type, options),
        (UInt32, Int16) => primitive_to_primitive_dyn::<u32, i16>(array, to_type, options),
        (UInt32, Int32) => primitive_to_primitive_dyn::<u32, i32>(array, to_type, options),
        (UInt32, Int64) => primitive_to_primitive_dyn::<u32, i64>(array, to_type, options),
        (UInt32, Float32) => primitive_to_primitive_dyn::<u32, f32>(array, to_type, as_options),
        (UInt32, Float64) => primitive_to_primitive_dyn::<u32, f64>(array, to_type, as_options),

        (UInt64, UInt8) => primitive_to_primitive_dyn::<u64, u8>(array, to_type, options),
        (UInt64, UInt16) => primitive_to_primitive_dyn::<u64, u16>(array, to_type, options),
        (UInt64, UInt32) => primitive_to_primitive_dyn::<u64, u32>(array, to_type, options),
        (UInt64, Int8) => primitive_to_primitive_dyn::<u64, i8>(array, to_type, options),
        (UInt64, Int16) => primitive_to_primitive_dyn::<u64, i16>(array, to_type, options),
        (UInt64, Int32) => primitive_to_primitive_dyn::<u64, i32>(array, to_type, options),
        (UInt64, Int64) => primitive_to_primitive_dyn::<u64, i64>(array, to_type, options),
        (UInt64, Float32) => primitive_to_primitive_dyn::<u64, f32>(array, to_type, as_options),
        (UInt64, Float64) => primitive_to_primitive_dyn::<u64, f64>(array, to_type, as_options),

        (Int8, UInt8) => primitive_to_primitive_dyn::<i8, u8>(array, to_type, options),
        (Int8, UInt16) => primitive_to_primitive_dyn::<i8, u16>(array, to_type, options),
        (Int8, UInt32) => primitive_to_primitive_dyn::<i8, u32>(array, to_type, options),
        (Int8, UInt64) => primitive_to_primitive_dyn::<i8, u64>(array, to_type, options),
        (Int8, Int16) => primitive_to_primitive_dyn::<i8, i16>(array, to_type, as_options),
        (Int8, Int32) => primitive_to_primitive_dyn::<i8, i32>(array, to_type, as_options),
        (Int8, Int64) => primitive_to_primitive_dyn::<i8, i64>(array, to_type, as_options),
        (Int8, Float32) => primitive_to_primitive_dyn::<i8, f32>(array, to_type, as_options),
        (Int8, Float64) => primitive_to_primitive_dyn::<i8, f64>(array, to_type, as_options),

        (Int16, UInt8) => primitive_to_primitive_dyn::<i16, u8>(array, to_type, options),
        (Int16, UInt16) => primitive_to_primitive_dyn::<i16, u16>(array, to_type, options),
        (Int16, UInt32) => primitive_to_primitive_dyn::<i16, u32>(array, to_type, options),
        (Int16, UInt64) => primitive_to_primitive_dyn::<i16, u64>(array, to_type, options),
        (Int16, Int8) => primitive_to_primitive_dyn::<i16, i8>(array, to_type, options),
        (Int16, Int32) => primitive_to_primitive_dyn::<i16, i32>(array, to_type, as_options),
        (Int16, Int64) => primitive_to_primitive_dyn::<i16, i64>(array, to_type, as_options),
        (Int16, Float32) => primitive_to_primitive_dyn::<i16, f32>(array, to_type, as_options),
        (Int16, Float64) => primitive_to_primitive_dyn::<i16, f64>(array, to_type, as_options),

        (Int32, UInt8) => primitive_to_primitive_dyn::<i32, u8>(array, to_type, options),
        (Int32, UInt16) => primitive_to_primitive_dyn::<i32, u16>(array, to_type, options),
        (Int32, UInt32) => primitive_to_primitive_dyn::<i32, u32>(array, to_type, options),
        (Int32, UInt64) => primitive_to_primitive_dyn::<i32, u64>(array, to_type, options),
        (Int32, Int8) => primitive_to_primitive_dyn::<i32, i8>(array, to_type, options),
        (Int32, Int16) => primitive_to_primitive_dyn::<i32, i16>(array, to_type, options),
        (Int32, Int64) => primitive_to_primitive_dyn::<i32, i64>(array, to_type, as_options),
        (Int32, Float32) => primitive_to_primitive_dyn::<i32, f32>(array, to_type, as_options),
        (Int32, Float64) => primitive_to_primitive_dyn::<i32, f64>(array, to_type, as_options),

        (Int64, UInt8) => primitive_to_primitive_dyn::<i64, u8>(array, to_type, options),
        (Int64, UInt16) => primitive_to_primitive_dyn::<i64, u16>(array, to_type, options),
        (Int64, UInt32) => primitive_to_primitive_dyn::<i64, u32>(array, to_type, options),
        (Int64, UInt64) => primitive_to_primitive_dyn::<i64, u64>(array, to_type, options),
        (Int64, Int8) => primitive_to_primitive_dyn::<i64, i8>(array, to_type, options),
        (Int64, Int16) => primitive_to_primitive_dyn::<i64, i16>(array, to_type, options),
        (Int64, Int32) => primitive_to_primitive_dyn::<i64, i32>(array, to_type, options),
        (Int64, Float32) => primitive_to_primitive_dyn::<i64, f32>(array, to_type, options),
        (Int64, Float64) => primitive_to_primitive_dyn::<i64, f64>(array, to_type, as_options),

        (Float32, UInt8) => primitive_to_primitive_dyn::<f32, u8>(array, to_type, options),
        (Float32, UInt16) => primitive_to_primitive_dyn::<f32, u16>(array, to_type, options),
        (Float32, UInt32) => primitive_to_primitive_dyn::<f32, u32>(array, to_type, options),
        (Float32, UInt64) => primitive_to_primitive_dyn::<f32, u64>(array, to_type, options),
        (Float32, Int8) => primitive_to_primitive_dyn::<f32, i8>(array, to_type, options),
        (Float32, Int16) => primitive_to_primitive_dyn::<f32, i16>(array, to_type, options),
        (Float32, Int32) => primitive_to_primitive_dyn::<f32, i32>(array, to_type, options),
        (Float32, Int64) => primitive_to_primitive_dyn::<f32, i64>(array, to_type, options),
        (Float32, Float64) => primitive_to_primitive_dyn::<f32, f64>(array, to_type, as_options),

        (Float64, UInt8) => primitive_to_primitive_dyn::<f64, u8>(array, to_type, options),
        (Float64, UInt16) => primitive_to_primitive_dyn::<f64, u16>(array, to_type, options),
        (Float64, UInt32) => primitive_to_primitive_dyn::<f64, u32>(array, to_type, options),
        (Float64, UInt64) => primitive_to_primitive_dyn::<f64, u64>(array, to_type, options),
        (Float64, Int8) => primitive_to_primitive_dyn::<f64, i8>(array, to_type, options),
        (Float64, Int16) => primitive_to_primitive_dyn::<f64, i16>(array, to_type, options),
        (Float64, Int32) => primitive_to_primitive_dyn::<f64, i32>(array, to_type, options),
        (Float64, Int64) => primitive_to_primitive_dyn::<f64, i64>(array, to_type, options),
        (Float64, Float32) => primitive_to_primitive_dyn::<f64, f32>(array, to_type, options),
        // end numeric casts

        // temporal casts
        (Int32, Date32) => primitive_to_same_primitive_dyn::<i32>(array, to_type),
        (Int32, Time32(TimeUnit::Second)) => primitive_to_same_primitive_dyn::<i32>(array, to_type),
        (Int32, Time32(TimeUnit::Millisecond)) => {
            primitive_to_same_primitive_dyn::<i32>(array, to_type)
        }
        // No support for microsecond/nanosecond with i32
        (Date32, Int32) => primitive_to_same_primitive_dyn::<i32>(array, to_type),
        (Time32(_), Int32) => primitive_to_same_primitive_dyn::<i32>(array, to_type),
        (Int64, Date64) => primitive_to_same_primitive_dyn::<i64>(array, to_type),
        // No support for second/milliseconds with i64
        (Int64, Time64(TimeUnit::Microsecond)) => {
            primitive_to_same_primitive_dyn::<i64>(array, to_type)
        }
        (Int64, Time64(TimeUnit::Nanosecond)) => {
            primitive_to_same_primitive_dyn::<i64>(array, to_type)
        }

        (Date64, Int64) => primitive_to_same_primitive_dyn::<i64>(array, to_type),
        (Time64(_), Int64) => primitive_to_same_primitive_dyn::<i64>(array, to_type),
        (Date32, Date64) => primitive_dyn!(array, date32_to_date64),
        (Date64, Date32) => primitive_dyn!(array, date64_to_date32),
        (Time32(TimeUnit::Second), Time32(TimeUnit::Millisecond)) => {
            primitive_dyn!(array, time32s_to_time32ms)
        }
        (Time32(TimeUnit::Millisecond), Time32(TimeUnit::Second)) => {
            primitive_dyn!(array, time32ms_to_time32s)
        }
        (Time32(from_unit), Time64(to_unit)) => {
            primitive_dyn!(array, time32_to_time64, from_unit, to_unit)
        }
        (Time64(TimeUnit::Microsecond), Time64(TimeUnit::Nanosecond)) => {
            primitive_dyn!(array, time64us_to_time64ns)
        }
        (Time64(TimeUnit::Nanosecond), Time64(TimeUnit::Microsecond)) => {
            primitive_dyn!(array, time64ns_to_time64us)
        }
        (Time64(from_unit), Time32(to_unit)) => {
            primitive_dyn!(array, time64_to_time32, from_unit, to_unit)
        }
        (Timestamp(_, _), Int64) => primitive_to_same_primitive_dyn::<i64>(array, to_type),
        (Int64, Timestamp(_, _)) => primitive_to_same_primitive_dyn::<i64>(array, to_type),
        (Timestamp(from_unit, tz1), Timestamp(to_unit, tz2)) if tz1 == tz2 => {
            primitive_dyn!(array, timestamp_to_timestamp, from_unit, to_unit, tz2)
        }
        (Timestamp(from_unit, _), Date32) => primitive_dyn!(array, timestamp_to_date32, from_unit),
        (Timestamp(from_unit, _), Date64) => primitive_dyn!(array, timestamp_to_date64, from_unit),

        (Int64, Duration(_)) => primitive_to_same_primitive_dyn::<i64>(array, to_type),
        (Duration(_), Int64) => primitive_to_same_primitive_dyn::<i64>(array, to_type),

        // null to primitive/flat types
        //(Null, Int32) => Ok(Box::new(Int32Array::from(vec![None; array.len()]))),
        (_, _) => Err(ArrowError::NotYetImplemented(format!(
            "Casting from {:?} to {:?} not supported",
            from_type, to_type,
        ))),
    }
}

/// Attempts to encode an array into an `ArrayDictionary` with index
/// type K and value (dictionary) type value_type
///
/// K is the key type
fn cast_to_dictionary<K: DictionaryKey>(
    array: &dyn Array,
    dict_value_type: &DataType,
    options: CastOptions,
) -> Result<Box<dyn Array>> {
    let array = cast_with_options(array, dict_value_type, options)?;
    let array = array.as_ref();
    match *dict_value_type {
        DataType::Int8 => primitive_to_dictionary_dyn::<i8, K>(array),
        DataType::Int16 => primitive_to_dictionary_dyn::<i16, K>(array),
        DataType::Int32 => primitive_to_dictionary_dyn::<i32, K>(array),
        DataType::Int64 => primitive_to_dictionary_dyn::<i64, K>(array),
        DataType::UInt8 => primitive_to_dictionary_dyn::<u8, K>(array),
        DataType::UInt16 => primitive_to_dictionary_dyn::<u16, K>(array),
        DataType::UInt32 => primitive_to_dictionary_dyn::<u32, K>(array),
        DataType::UInt64 => primitive_to_dictionary_dyn::<u64, K>(array),
        DataType::Utf8 => utf8_to_dictionary_dyn::<i32, K>(array),
        DataType::LargeUtf8 => utf8_to_dictionary_dyn::<i64, K>(array),
        _ => Err(ArrowError::NotYetImplemented(format!(
            "Unsupported output type for dictionary packing: {:?}",
            dict_value_type
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NativeType;

    #[test]
    fn i32_to_f64() {
        let array = Int32Array::from_slice(&[5, 6, 7, 8, 9]);
        let b = cast(&array, &DataType::Float64).unwrap();
        let c = b.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((5.0 - c.value(0)).abs() < f64::EPSILON);
        assert!((6.0 - c.value(1)).abs() < f64::EPSILON);
        assert!((7.0 - c.value(2)).abs() < f64::EPSILON);
        assert!((8.0 - c.value(3)).abs() < f64::EPSILON);
        assert!((9.0 - c.value(4)).abs() < f64::EPSILON);
    }

    #[test]
    fn i32_as_f64_no_overflow() {
        let array = Int32Array::from_slice(&[5, 6, 7, 8, 9]);
        let b = wrapped_cast(&array, &DataType::Float64).unwrap();
        let c = b.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((5.0 - c.value(0)).abs() < f64::EPSILON);
        assert!((6.0 - c.value(1)).abs() < f64::EPSILON);
        assert!((7.0 - c.value(2)).abs() < f64::EPSILON);
        assert!((8.0 - c.value(3)).abs() < f64::EPSILON);
        assert!((9.0 - c.value(4)).abs() < f64::EPSILON);
    }

    #[test]
    fn u16_as_u8_overflow() {
        let array = UInt16Array::from_slice(&[255, 256, 257, 258, 259]);
        let b = wrapped_cast(&array, &DataType::UInt8).unwrap();
        let c = b.as_any().downcast_ref::<UInt8Array>().unwrap();
        let values = c.values().as_slice();

        println!("{}", 255u8.wrapping_add(10));

        assert_eq!(values, &[255, 0, 1, 2, 3])
    }

    #[test]
    fn u16_as_u8_no_overflow() {
        let array = UInt16Array::from_slice(&[1, 2, 3, 4, 5]);
        let b = wrapped_cast(&array, &DataType::UInt8).unwrap();
        let c = b.as_any().downcast_ref::<UInt8Array>().unwrap();
        let values = c.values().as_slice();
        assert_eq!(values, &[1, 2, 3, 4, 5])
    }

    #[test]
    fn float_range_max() {
        //floats to integers
        let u: Option<u32> = num::cast(f32::MAX);
        assert_eq!(u, None);
        let u: Option<u64> = num::cast(f32::MAX);
        assert_eq!(u, None);

        let u: Option<i32> = num::cast(f32::MAX);
        assert_eq!(u, None);
        let u: Option<i64> = num::cast(f32::MAX);
        assert_eq!(u, None);

        let u: Option<u32> = num::cast(f64::MAX);
        assert_eq!(u, None);
        let u: Option<u64> = num::cast(f64::MAX);
        assert_eq!(u, None);

        let u: Option<i32> = num::cast(f64::MAX);
        assert_eq!(u, None);
        let u: Option<i64> = num::cast(f64::MAX);
        assert_eq!(u, None);

        //integers to floats
        let u: Option<f32> = num::cast(u32::MAX);
        assert!(u.is_some());
        let u: Option<f64> = num::cast(u32::MAX);
        assert!(u.is_some());

        let u: Option<f32> = num::cast(i32::MAX);
        assert!(u.is_some());
        let u: Option<f64> = num::cast(i32::MAX);
        assert!(u.is_some());

        let u: Option<f64> = num::cast(i64::MAX);
        assert!(u.is_some());
        let u: Option<f64> = num::cast(u64::MAX);
        assert!(u.is_some());

        let u: Option<f64> = num::cast(f32::MAX);
        assert!(u.is_some());
    }

    #[test]
    fn float_range_min() {
        //floats to integers
        let u: Option<u32> = num::cast(f32::MIN);
        assert_eq!(u, None);
        let u: Option<u64> = num::cast(f32::MIN);
        assert_eq!(u, None);

        let u: Option<i32> = num::cast(f32::MIN);
        assert_eq!(u, None);
        let u: Option<i64> = num::cast(f32::MIN);
        assert_eq!(u, None);

        let u: Option<u32> = num::cast(f64::MIN);
        assert_eq!(u, None);
        let u: Option<u64> = num::cast(f64::MIN);
        assert_eq!(u, None);

        let u: Option<i32> = num::cast(f64::MIN);
        assert_eq!(u, None);
        let u: Option<i64> = num::cast(f64::MIN);
        assert_eq!(u, None);

        //integers to floats
        let u: Option<f32> = num::cast(u32::MIN);
        assert!(u.is_some());
        let u: Option<f64> = num::cast(u32::MIN);
        assert!(u.is_some());

        let u: Option<f32> = num::cast(i32::MIN);
        assert!(u.is_some());
        let u: Option<f64> = num::cast(i32::MIN);
        assert!(u.is_some());

        let u: Option<f64> = num::cast(i64::MIN);
        assert!(u.is_some());
        let u: Option<f64> = num::cast(u64::MIN);
        assert!(u.is_some());

        let u: Option<f64> = num::cast(f32::MIN);
        assert!(u.is_some());
    }

    #[test]
    fn f32_as_u8_overflow() {
        let array = Float32Array::from_slice(&[1.1, 5000.0]);
        let b = cast(&array, &DataType::UInt8).unwrap();
        let c = b.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from(&[Some(1), None]);
        assert_eq!(c, &expected);

        let b = cast_with_options(&array, &DataType::UInt8, CastOptions { wrapped: true }).unwrap();
        let c = b.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from(&[Some(1), Some(255)]);
        assert_eq!(c, &expected);
    }

    #[test]
    fn i32_to_u8() {
        let array = Int32Array::from_slice(&[-5, 6, -7, 8, 100000000]);
        let b = cast(&array, &DataType::UInt8).unwrap();
        let expected = UInt8Array::from(&[None, Some(6), None, Some(8), None]);
        let c = b.as_any().downcast_ref::<UInt8Array>().unwrap();
        assert_eq!(c, &expected);
    }

    #[test]
    fn i32_to_u8_sliced() {
        let array = Int32Array::from_slice(&[-5, 6, -7, 8, 100000000]);
        let array = array.slice(2, 3);
        let b = cast(&array, &DataType::UInt8).unwrap();
        let expected = UInt8Array::from(&[None, Some(8), None]);
        let c = b.as_any().downcast_ref::<UInt8Array>().unwrap();
        assert_eq!(c, &expected);
    }

    #[test]
    fn i32_to_i32() {
        let array = Int32Array::from_slice(&[5, 6, 7, 8, 9]);
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = &[5, 6, 7, 8, 9];
        let expected = Int32Array::from_slice(expected);
        assert_eq!(c, &expected);
    }

    #[test]
    fn i32_to_list_i32() {
        let array = Int32Array::from_slice(&[5, 6, 7, 8, 9]);
        let b = cast(
            &array,
            &DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
        )
        .unwrap();

        let arr = b.as_any().downcast_ref::<ListArray<i32>>().unwrap();
        assert_eq!(&[0, 1, 2, 3, 4, 5], arr.offsets().as_slice());
        let values = arr.values();
        let c = values
            .as_any()
            .downcast_ref::<PrimitiveArray<i32>>()
            .unwrap();

        let expected = Int32Array::from_slice(&[5, 6, 7, 8, 9]);
        assert_eq!(c, &expected);
    }

    #[test]
    fn i32_to_list_i32_nullable() {
        let input = [Some(5), None, Some(7), Some(8), Some(9)];

        let array = Int32Array::from(input);
        let b = cast(
            &array,
            &DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
        )
        .unwrap();

        let arr = b.as_any().downcast_ref::<ListArray<i32>>().unwrap();
        assert_eq!(&[0, 1, 2, 3, 4, 5], arr.offsets().as_slice());
        let values = arr.values();
        let c = values.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = &[Some(5), None, Some(7), Some(8), Some(9)];
        let expected = Int32Array::from(expected);
        assert_eq!(c, &expected);
    }

    #[test]
    fn i32_to_list_f64_nullable_sliced() {
        let input = [Some(5), None, Some(7), Some(8), None, Some(10)];

        let array = Int32Array::from(input);

        let array = array.slice(2, 4);
        let b = cast(
            &array,
            &DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
        )
        .unwrap();

        let arr = b.as_any().downcast_ref::<ListArray<i32>>().unwrap();
        assert_eq!(&[0, 1, 2, 3, 4], arr.offsets().as_slice());
        let values = arr.values();
        let c = values.as_any().downcast_ref::<Float64Array>().unwrap();

        let expected = &[Some(7.0), Some(8.0), None, Some(10.0)];
        let expected = Float64Array::from(expected);
        assert_eq!(c, &expected);
    }

    #[test]
    fn utf8_to_i32() {
        let array = Utf8Array::<i32>::from_slice(&["5", "6", "seven", "8", "9.1"]);
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();

        let expected = &[Some(5), Some(6), None, Some(8), None];
        let expected = Int32Array::from(expected);
        assert_eq!(c, &expected);
    }

    #[test]
    fn bool_to_i32() {
        let array = BooleanArray::from(vec![Some(true), Some(false), None]);
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = &[Some(1), Some(0), None];
        let expected = Int32Array::from(expected);
        assert_eq!(c, &expected);
    }

    #[test]
    fn bool_to_f64() {
        let array = BooleanArray::from(vec![Some(true), Some(false), None]);
        let b = cast(&array, &DataType::Float64).unwrap();
        let c = b.as_any().downcast_ref::<Float64Array>().unwrap();

        let expected = &[Some(1.0), Some(0.0), None];
        let expected = Float64Array::from(expected);
        assert_eq!(c, &expected);
    }

    #[test]
    #[should_panic(expected = "Casting from Int32 to Timestamp(Microsecond, None) not supported")]
    fn int32_to_timestamp() {
        let array = Int32Array::from(&[Some(2), Some(10), None]);
        cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();
    }

    #[test]
    fn consistency() {
        use crate::datatypes::DataType::*;
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
            List(Box::new(Field::new("a", Utf8, true))),
            LargeList(Box::new(Field::new("a", Utf8, true))),
        ];
        datatypes
            .clone()
            .into_iter()
            .zip(datatypes.into_iter())
            .for_each(|(d1, d2)| {
                let array = new_null_array(d1.clone(), 10);
                if can_cast_types(&d1, &d2) {
                    let result = cast(array.as_ref(), &d2);
                    if let Ok(result) = result {
                        assert_eq!(result.data_type(), &d2);
                    } else {
                        panic!("Cast should have not failed")
                    }
                } else {
                    assert!(
                        cast_with_options(array.as_ref(), &d2, CastOptions::default()).is_err()
                    );
                }
            });
    }

    fn test_primitive_to_primitive<I: NativeType, O: NativeType>(
        lhs: &[I],
        lhs_type: DataType,
        expected: &[O],
        expected_type: DataType,
    ) {
        let a = PrimitiveArray::<I>::from_slice(lhs).to(lhs_type);
        let b = cast(&a, &expected_type).unwrap();
        let b = b.as_any().downcast_ref::<PrimitiveArray<O>>().unwrap();
        let expected = PrimitiveArray::<O>::from_slice(expected).to(expected_type);
        assert_eq!(b, &expected);
    }

    #[test]
    fn date32_to_date64() {
        test_primitive_to_primitive(
            &[10000i32, 17890],
            DataType::Date32,
            &[864000000000i64, 1545696000000],
            DataType::Date64,
        );
    }

    #[test]
    fn date64_to_date32() {
        test_primitive_to_primitive(
            &[864000000005i64, 1545696000001],
            DataType::Date64,
            &[10000i32, 17890],
            DataType::Date32,
        );
    }

    #[test]
    fn date32_to_int32() {
        test_primitive_to_primitive(
            &[10000i32, 17890],
            DataType::Date32,
            &[10000i32, 17890],
            DataType::Int32,
        );
    }

    #[test]
    fn int32_to_date32() {
        test_primitive_to_primitive(
            &[10000i32, 17890],
            DataType::Int32,
            &[10000i32, 17890],
            DataType::Date32,
        );
    }

    #[test]
    fn timestamp_to_date32() {
        test_primitive_to_primitive(
            &[864000000005i64, 1545696000001],
            DataType::Timestamp(TimeUnit::Millisecond, Some(String::from("UTC"))),
            &[10000i32, 17890],
            DataType::Date32,
        );
    }

    #[test]
    fn timestamp_to_date64() {
        test_primitive_to_primitive(
            &[864000000005i64, 1545696000001],
            DataType::Timestamp(TimeUnit::Millisecond, Some(String::from("UTC"))),
            &[864000000005i64, 1545696000001i64],
            DataType::Date64,
        );
    }

    #[test]
    fn timestamp_to_i64() {
        test_primitive_to_primitive(
            &[864000000005i64, 1545696000001],
            DataType::Timestamp(TimeUnit::Millisecond, Some(String::from("UTC"))),
            &[864000000005i64, 1545696000001i64],
            DataType::Int64,
        );
    }

    #[test]
    fn timestamp_to_timestamp() {
        test_primitive_to_primitive(
            &[864000003005i64, 1545696002001],
            DataType::Timestamp(TimeUnit::Millisecond, None),
            &[864000003i64, 1545696002],
            DataType::Timestamp(TimeUnit::Second, None),
        );
    }

    #[test]
    fn utf8_to_dict() {
        let array = Utf8Array::<i32>::from(&[Some("one"), None, Some("three"), Some("one")]);

        // Cast to a dictionary (same value type, Utf8)
        let cast_type = DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8));
        let result = cast(&array, &cast_type).expect("cast failed");

        let mut expected = MutableDictionaryArray::<u8, MutableUtf8Array<i32>>::new();
        expected
            .try_extend([Some("one"), None, Some("three"), Some("one")])
            .unwrap();
        let expected: DictionaryArray<u8> = expected.into();
        assert_eq!(expected, result.as_ref());
    }

    #[test]
    fn dict_to_utf8() {
        let mut array = MutableDictionaryArray::<u8, MutableUtf8Array<i32>>::new();
        array
            .try_extend([Some("one"), None, Some("three"), Some("one")])
            .unwrap();
        let array: DictionaryArray<u8> = array.into();

        let result = cast(&array, &DataType::Utf8).expect("cast failed");

        let expected = Utf8Array::<i32>::from(&[Some("one"), None, Some("three"), Some("one")]);

        assert_eq!(expected, result.as_ref());
    }

    #[test]
    fn i32_to_dict() {
        let array = Int32Array::from(&[Some(1), None, Some(3), Some(1)]);

        // Cast to a dictionary (same value type, Utf8)
        let cast_type = DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Int32));
        let result = cast(&array, &cast_type).expect("cast failed");

        let mut expected = MutableDictionaryArray::<u8, MutablePrimitiveArray<i32>>::new();
        expected
            .try_extend([Some(1), None, Some(3), Some(1)])
            .unwrap();
        let expected: DictionaryArray<u8> = expected.into();
        assert_eq!(expected, result.as_ref());
    }

    fn list_to_list() {
        let data = vec![
            Some(vec![Some(1i32), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), None, Some(6)]),
        ];

        let expected_data = data
            .iter()
            .map(|x| x.as_ref().map(|x| x.iter().map(|x| x.map(|x| x as u16))));

        let mut array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
        array.try_extend(data.clone()).unwrap();
        let array: ListArray<i32> = array.into();

        let mut expected = MutableListArray::<i32, MutablePrimitiveArray<u16>>::new();
        expected.try_extend(expected_data).unwrap();
        let expected: ListArray<i32> = expected.into();

        let result = cast(&array, expected.data_type()).unwrap();
        assert_eq!(expected, result.as_ref());
    }

    /*
    #[test]
    fn dict_to_dict_bad_index_value_primitive() {
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
        let res = cast_with_options(&array, &cast_type);
        assert, CastOptions::default())!(res.is_err());
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
    fn dict_to_dict_bad_index_value_utf8() {
        use DataType::*;
        // Same test as dict_to_dict_bad_index_value but use
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
        let res = cast_with_options(&array, &cast_type);
        assert, CastOptions::default())!(res.is_err());
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
    fn utf8_to_date32() {
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
        let b = cast_with_options(&array, &DataType::Date32, CastOptions::default()).unwrap();
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
    fn utf8_to_date64() {
        let a = StringArray::from(vec![
            "2000-01-01T12:00:00", // date + time valid
            "2020-12-15T12:34:56", // date + time valid
            "2020-2-2T12:34:56",   // valid date time without leading 0s
            "2000-00-00T12:00:00", // invalid month and day
            "2000-01-01 12:00:00", // missing the 'T'
            "2000-01-01",          // just a date is invalid
        ]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast_with_options(&array, &DataType::Date64, CastOptions::default()).unwrap();
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

    fn make_union_array() -> UnionArray {
        let mut builder = UnionBuilder::new_dense(7);
        builder.append::<i32>("a", 1).unwrap();
        builder.append::<i64>("b", 2).unwrap();
        builder.build().unwrap()
    }
    */
}
