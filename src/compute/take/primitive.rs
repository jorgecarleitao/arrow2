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
    array::{Array, PrimitiveArray},
    bitmap::{Bitmap, MutableBitmap},
    buffer::{Buffer, MutableBuffer},
    error::Result,
    types::NativeType,
};

use super::Index;

// take implementation when neither values nor indices contain nulls
fn take_no_validity<T: NativeType, I: Index>(
    values: &[T],
    indices: &[I],
) -> (Buffer<T>, Option<Bitmap>) {
    let values = indices.iter().map(|index| values[index.to_usize()]);
    let buffer = MutableBuffer::from_trusted_len_iter(values);

    (buffer.into(), None)
}

// take implementation when only values contain nulls
fn take_values_validity<T: NativeType, I: Index>(
    values: &PrimitiveArray<T>,
    indices: &[I],
) -> (Buffer<T>, Option<Bitmap>) {
    let mut null = MutableBitmap::with_capacity(indices.len());

    let null_values = values.validity().as_ref().unwrap();

    let values_values = values.values();

    let values = indices.iter().map(|index| {
        let index = index.to_usize();
        if null_values.get_bit(index) {
            null.push(true);
        } else {
            null.push(false);
        }
        values_values[index]
    });
    let buffer = MutableBuffer::from_trusted_len_iter(values);

    (buffer.into(), null.into())
}

// take implementation when only indices contain nulls
fn take_indices_validity<T: NativeType, I: Index>(
    values: &[T],
    indices: &PrimitiveArray<I>,
) -> (Buffer<T>, Option<Bitmap>) {
    let validity = indices.validity().as_ref().unwrap();
    let values = indices.values().iter().map(|index| {
        let index = index.to_usize();
        match values.get(index) {
            Some(value) => *value,
            None => {
                if !validity.get_bit(index) {
                    T::default()
                } else {
                    panic!("Out-of-bounds index {}", index)
                }
            }
        }
    });

    let buffer = MutableBuffer::from_trusted_len_iter(values);

    (buffer.into(), indices.validity().clone())
}

// take implementation when both values and indices contain nulls
fn take_values_indices_validity<T: NativeType, I: Index>(
    values: &PrimitiveArray<T>,
    indices: &PrimitiveArray<I>,
) -> (Buffer<T>, Option<Bitmap>) {
    let mut bitmap = MutableBitmap::with_capacity(indices.len());

    let values_validity = values.validity().as_ref().unwrap();

    let values_values = values.values();
    let values = indices.iter().map(|index| match index {
        Some(index) => {
            let index = index.to_usize();
            bitmap.push(values_validity.get_bit(index));
            values_values[index]
        }
        None => {
            bitmap.push(false);
            T::default()
        }
    });
    let buffer = MutableBuffer::from_trusted_len_iter(values);
    (buffer.into(), bitmap.into())
}

/// `take` implementation for primitive arrays
pub fn take<T: NativeType, I: Index>(
    values: &PrimitiveArray<T>,
    indices: &PrimitiveArray<I>,
) -> Result<PrimitiveArray<T>> {
    let indices_has_validity = indices.null_count() > 0;
    let values_has_validity = values.null_count() > 0;
    let (buffer, validity) = match (values_has_validity, indices_has_validity) {
        (false, false) => take_no_validity::<T, I>(values.values(), indices.values()),
        (true, false) => take_values_validity::<T, I>(values, indices.values()),
        (false, true) => take_indices_validity::<T, I>(values.values(), indices),
        (true, true) => take_values_indices_validity::<T, I>(values, indices),
    };

    Ok(PrimitiveArray::<T>::from_data(
        values.data_type().clone(),
        buffer,
        validity,
    ))
}
