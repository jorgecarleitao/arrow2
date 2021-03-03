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

use std::hash::Hash;

use crate::{
    array::{
        Array, BooleanArray, DictionaryKey, DictionaryPrimitive, Offset, Primitive, PrimitiveArray,
        TryFromIterator, Utf8Array,
    },
    buffer::Bitmap,
    datatypes::DataType,
    types::NativeType,
};
use crate::{error::Result, util::lexical_to_string};

use super::cast;

/// Cast numeric types to Utf8
pub fn cast_numeric_to_string<T, O>(array: &dyn Array) -> Result<Box<dyn Array>>
where
    O: Offset,
    T: NativeType + lexical_core::ToLexical,
{
    let array = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

    let iter = array.iter().map(|x| x.map(lexical_to_string));

    let array = unsafe { Utf8Array::<O>::from_trusted_len_iter(iter) };

    Ok(Box::new(array))
}

/// Convert Array into a PrimitiveArray of type, and apply numeric cast
pub fn cast_numeric_arrays<I, O>(from: &dyn Array, to_type: &DataType) -> Result<Box<dyn Array>>
where
    I: NativeType + num::NumCast,
    O: NativeType + num::NumCast,
{
    let from = from.as_any().downcast_ref::<PrimitiveArray<I>>().unwrap();
    Ok(Box::new(cast_typed_primitive::<I, O>(from, to_type)))
}

/// Cast PrimitiveArray to PrimitiveArray
pub fn cast_typed_primitive<I, O>(from: &PrimitiveArray<I>, to_type: &DataType) -> PrimitiveArray<O>
where
    I: NativeType + num::NumCast,
    O: NativeType + num::NumCast,
{
    let from = from.as_any().downcast_ref::<PrimitiveArray<I>>().unwrap();

    let iter = from.iter().map(|v| v.and_then(num::cast::cast::<I, O>));
    // Soundness:
    //  The iterator is trustedLen because it comes from an `PrimitiveArray`.
    unsafe { Primitive::<O>::from_trusted_len_iter(iter) }.to(to_type.clone())
}

/// Cast an array by changing its data type to the desired type
pub fn cast_array_data<T>(from: &dyn Array, to_type: &DataType) -> Result<Box<dyn Array>>
where
    T: NativeType,
{
    let from = from.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

    Ok(Box::new(PrimitiveArray::<T>::from_data(
        to_type.clone(),
        from.values_buffer().clone(),
        from.validity().clone(),
    )))
}

/// Cast numeric types to Boolean
///
/// Any zero value returns `false` while non-zero returns `true`
pub fn cast_numeric_to_bool<T>(array: &dyn Array) -> Result<Box<dyn Array>>
where
    T: NativeType,
{
    let array = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

    let iter = array.values().iter().map(|v| *v != T::default());
    let values = unsafe { Bitmap::from_trusted_len_iter(iter) };

    let array = BooleanArray::from_data(values, array.validity().clone());

    Ok(Box::new(array))
}

pub fn primitive_to_dictionary<T: NativeType + Eq + Hash, K: DictionaryKey>(
    array: &dyn Array,
    to: &DataType,
) -> Result<Box<dyn Array>> {
    let values = cast(array, to)?;
    let values = values.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

    let iter = values.iter().map(|x| Result::Ok(x));
    let primitive = DictionaryPrimitive::<K, Primitive<T>, _>::try_from_iter(iter)?;

    let array = primitive.to(DataType::Dictionary(
        Box::new(K::DATA_TYPE),
        Box::new(values.data_type().clone()),
    ));

    Ok(Box::new(array))
}
