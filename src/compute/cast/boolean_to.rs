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
    array::{Array, BooleanArray, Primitive},
    datatypes::DataType,
    types::NativeType,
};
use crate::{
    array::{Offset, Utf8Array},
    error::Result,
};

/// Cast Boolean types to numeric
///
/// `false` returns 0 while `true` returns 1
pub fn cast_bool_to_numeric<T>(array: &dyn Array, to: &DataType) -> Result<Box<dyn Array>>
where
    T: NativeType + num::One,
{
    let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();

    let iter = array
        .iter()
        .map(|x| x.map(|x| if x { T::one() } else { T::default() }));

    // Soundness:
    //     The iterator is trustedLen
    let array = unsafe { Primitive::<T>::from_trusted_len_iter(iter) }.to(to.clone());

    Ok(Box::new(array))
}

/// Cast Boolean types to numeric
///
/// `false` returns 0 while `true` returns 1
pub fn cast_bool_to_utf8<O: Offset>(array: &dyn Array) -> Result<Box<dyn Array>> {
    let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();

    let iter = array.iter().map(|x| x.map(|x| if x { "1" } else { "0" }));

    // Soundness:
    //     The iterator is trustedLen
    let array = unsafe { Utf8Array::<O>::from_trusted_len_iter(iter) };

    Ok(Box::new(array))
}
