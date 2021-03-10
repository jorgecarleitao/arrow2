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

use chrono::Datelike;

use crate::{
    array::{
        Array, DictionaryKey, DictionaryPrimitive, Offset, Primitive, PrimitiveArray,
        TryFromIterator, Utf8Array, Utf8Primitive,
    },
    datatypes::DataType,
    types::NativeType,
};
use crate::{error::Result, temporal_conversions::EPOCH_DAYS_FROM_CE};

use super::cast;

/// Cast numeric types to Utf8
pub fn cast_string_to_numeric<O: Offset, T>(
    from: &dyn Array,
    to: &DataType,
) -> Result<Box<dyn Array>>
where
    T: NativeType + lexical_core::FromLexical,
{
    let from = from.as_any().downcast_ref::<Utf8Array<O>>().unwrap();

    let iter = from
        .iter()
        .map(|x| x.and_then::<T, _>(|x| lexical_core::parse(x.as_bytes()).ok()));

    // Benefit:
    //     20% performance improvement
    // Soundness:
    //     The iterator is trustedLen because it comes from an `StringArray`.
    let array = unsafe { Primitive::<T>::from_trusted_len_iter(iter) }.to(to.clone());

    Ok(Box::new(array))
}

pub fn to_date32<O: Offset>(array: &dyn Array, to_type: &DataType) -> PrimitiveArray<i32> {
    let array = array.as_any().downcast_ref::<Utf8Array<O>>().unwrap();

    let iter = array.iter().map(|x| {
        x.and_then(|x| {
            x.parse::<chrono::NaiveDate>()
                .ok()
                .map(|x| x.num_days_from_ce() - EPOCH_DAYS_FROM_CE)
        })
    });
    // Soundness:
    //     The iterator is trustedLen because it comes from a `Utf8Array`.
    unsafe { Primitive::<i32>::from_trusted_len_iter(iter) }.to(to_type.clone())
}

pub fn to_date64<O: Offset>(array: &dyn Array, to_type: &DataType) -> PrimitiveArray<i64> {
    let array = array.as_any().downcast_ref::<Utf8Array<O>>().unwrap();

    let iter = array.iter().map(|x| {
        x.and_then(|x| {
            x.parse::<chrono::NaiveDateTime>()
                .ok()
                .map(|x| x.timestamp_millis())
        })
    });
    // Soundness:
    //     The iterator is trustedLen because it comes from a `Utf8Array`.
    unsafe { Primitive::<i64>::from_trusted_len_iter(iter) }.to(to_type.clone())
}

// Packs the data as a StringDictionaryArray, if possible, with the
// key types of K
pub fn string_to_dictionary<O: Offset, K: DictionaryKey>(
    array: &dyn Array,
) -> Result<Box<dyn Array>> {
    let to = if O::is_large() {
        DataType::LargeUtf8
    } else {
        DataType::Utf8
    };

    let values = cast(array, &to)?;
    let values = values.as_any().downcast_ref::<Utf8Array<O>>().unwrap();

    let iter = values.iter().map(Result::Ok);
    let primitive = DictionaryPrimitive::<K, Utf8Primitive<i32>, _>::try_from_iter(iter)?;

    let array = primitive.to(DataType::Dictionary(Box::new(K::DATA_TYPE), Box::new(to)));

    Ok(Box::new(array))
}
