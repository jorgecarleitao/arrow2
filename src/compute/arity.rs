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

//! Defines kernels suitable to perform operations to primitive arrays.

use super::utils::combine_validities;
use crate::error::{ArrowError, Result};

use crate::buffer::Buffer;
use crate::{
    array::{Array, PrimitiveArray},
    buffer::NativeType,
    datatypes::DataType,
};

/// Applies an unary and infalible function to a primitive array.
/// This is the fastest way to perform an operation on a primitive array when
/// the benefits of a vectorized operation outweights the cost of branching nulls and non-nulls.
/// # Implementation
/// This will apply the function for all values, including those on null slots.
/// This implies that the operation must be infalible for any value of the corresponding type
/// or this function may panic.
#[inline]
pub fn unary<I, F, O>(array: &PrimitiveArray<I>, op: F, data_type: &DataType) -> PrimitiveArray<O>
where
    I: NativeType,
    O: NativeType,
    F: Fn(I) -> O,
{
    let values = array.values().iter().map(|v| op(*v));
    // JUSTIFICATION
    //  Benefit
    //      ~60% speedup
    //  Soundness
    //      `values` is an iterator with a known size because arrays are sized.
    let values = unsafe { Buffer::from_trusted_len_iter(values) };

    PrimitiveArray::<O>::from_data(data_type.clone(), values, array.validity().clone())
}

#[inline]
pub fn binary<T, F>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
    op: F,
) -> Result<PrimitiveArray<T>>
where
    T: NativeType,
    F: Fn(T, T) -> T,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arays must have the same logical type".to_string(),
        ));
    }
    if lhs.len() != rhs.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same length".to_string(),
        ));
    }

    let validity = combine_validities(lhs.validity(), rhs.validity());

    let values = lhs
        .values()
        .iter()
        .zip(rhs.values().iter())
        .map(|(l, r)| op(*l, *r));
    // JUSTIFICATION
    //  Benefit
    //      ~60% speedup
    //  Soundness
    //      `values` is an iterator with a known size.
    let values = unsafe { Buffer::from_trusted_len_iter(values) };

    Ok(PrimitiveArray::<T>::from_data(
        lhs.data_type().clone(),
        values,
        validity,
    ))
}

pub fn try_binary<E, T, F>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
    op: F,
) -> Result<PrimitiveArray<T>>
where
    T: NativeType,
    F: Fn(T, T) -> Result<T>,
{
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Arays must have the same logical type".to_string(),
        ));
    }
    if lhs.len() != rhs.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same length".to_string(),
        ));
    }

    let validity = combine_validities(lhs.validity(), rhs.validity());

    let values =
        lhs.values().iter().zip(rhs.values().iter()).map(|(l, r)| {
            op(*l, *r).map_err(|error| ArrowError::from_external_error(Box::new(error)))
        });
    // JUSTIFICATION
    //  Benefit
    //      ~60% speedup
    //  Soundness
    //      `values` is an iterator with a known size.
    let values = unsafe { Buffer::try_from_trusted_len_iter(values) }?;

    Ok(PrimitiveArray::<T>::from_data(
        lhs.data_type().clone(),
        values,
        validity,
    ))
}
