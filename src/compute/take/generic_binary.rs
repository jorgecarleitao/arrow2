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
    array::{Array, GenericBinaryArray, Offset, PrimitiveArray},
    buffer::{Bitmap, Buffer, MutableBitmap, MutableBuffer},
    error::Result,
};

use super::maybe_usize;

pub fn take_values<O: Offset>(
    length: O,
    starts: &[O],
    offsets: &[O],
    values: &[u8],
) -> Result<Buffer<u8>> {
    let new_len = maybe_usize::<O>(length)?;
    let mut buffer = MutableBuffer::with_capacity(new_len);
    starts
        .iter()
        .zip(offsets.windows(2))
        .try_for_each(|(start_, window)| {
            let start = maybe_usize::<O>(*start_)?;
            let end = maybe_usize::<O>(*start_ + (window[1] - window[0]))?;
            buffer.extend_from_slice(&values[start..end]);
            Result::Ok(())
        })?;
    Ok(buffer.into())
}

// take implementation when neither values nor indices contain nulls
pub fn take_no_validity<O: Offset, I: Offset>(
    offsets: &[O],
    values: &[u8],
    indices: &[I],
) -> Result<(Buffer<O>, Buffer<u8>, Option<Bitmap>)> {
    let mut length = O::default();
    let mut starts = MutableBuffer::<O>::with_capacity(indices.len());
    let offsets = indices.iter().map(|index| {
        let index = maybe_usize::<I>(*index)?;
        let start = offsets[index];
        length += offsets[index + 1] - start;
        starts.push(start);
        Result::Ok(length)
    });
    let offsets = std::iter::once(Ok(O::default())).chain(offsets);
    // Soundness: `TrustedLen`.
    let offsets = unsafe { Buffer::try_from_trusted_len_iter(offsets)? };
    let starts: Buffer<O> = starts.into();

    let buffer = take_values(length, starts.as_slice(), offsets.as_slice(), values)?;

    Ok((offsets, buffer, None))
}

// take implementation when only values contain nulls
pub fn take_values_validity<O: Offset, I: Offset, A: GenericBinaryArray<O>>(
    values: &A,
    indices: &[I],
) -> Result<(Buffer<O>, Buffer<u8>, Option<Bitmap>)> {
    let mut length = O::default();
    let mut validity = MutableBitmap::with_capacity(indices.len());

    let null_values = values.validity().as_ref().unwrap();
    let offsets = values.offsets();
    let values_values = values.values();

    let mut starts = MutableBuffer::<O>::with_capacity(indices.len());
    let offsets = indices.iter().map(|index| {
        let index = maybe_usize::<I>(*index)?;
        if null_values.get_bit(index) {
            validity.push(true);
            let start = offsets[index];
            length += offsets[index + 1] - start;
            starts.push(start);
        } else {
            validity.push(false);
            starts.push(O::default());
        }
        Result::Ok(length)
    });
    let offsets = std::iter::once(Ok(O::default())).chain(offsets);
    // Soundness: `TrustedLen`.
    let offsets = unsafe { Buffer::try_from_trusted_len_iter(offsets) }?;
    let starts: Buffer<O> = starts.into();

    let buffer = take_values(length, starts.as_slice(), offsets.as_slice(), values_values)?;

    Ok((offsets, buffer, validity.into()))
}

// take implementation when only indices contain nulls
pub fn take_indices_validity<O: Offset, I: Offset>(
    offsets: &[O],
    values: &[u8],
    indices: &PrimitiveArray<I>,
) -> Result<(Buffer<O>, Buffer<u8>, Option<Bitmap>)> {
    let mut length = O::default();
    let null_indices = indices.validity().as_ref().unwrap();

    let mut starts = MutableBuffer::<O>::with_capacity(indices.len());
    let offsets = indices.values().iter().map(|index| {
        let index = maybe_usize::<I>(*index)?;
        if null_indices.get_bit(index) {
            let start = offsets[index];
            length += offsets[index + 1] - start;
            starts.push(start);
        } else {
            starts.push(O::default());
        }
        Result::Ok(length)
    });
    let offsets = std::iter::once(Ok(O::default())).chain(offsets);
    let offsets = unsafe { Buffer::try_from_trusted_len_iter(offsets) }?;
    let starts: Buffer<O> = starts.into();

    let buffer = take_values(length, starts.as_slice(), offsets.as_slice(), values)?;

    Ok((offsets, buffer, indices.validity().clone()))
}

// take implementation when both indices and values contain nulls
pub fn take_values_indices_validity<O: Offset, I: Offset, A: GenericBinaryArray<O>>(
    values: &A,
    indices: &PrimitiveArray<I>,
) -> Result<(Buffer<O>, Buffer<u8>, Option<Bitmap>)> {
    let mut length = O::default();
    let mut validity = MutableBitmap::with_capacity(indices.len());

    let values_validity = values.validity().as_ref().unwrap();
    let offsets = values.offsets();
    let values_values = values.values();

    let mut starts = MutableBuffer::<O>::with_capacity(indices.len());
    let offsets = indices.iter().map(|index| {
        match index {
            Some(index) => {
                let index = maybe_usize::<I>(index)?;
                if values_validity.get_bit(index) {
                    validity.push(true);
                    length += offsets[index + 1] - offsets[index];
                    starts.push(offsets[index]);
                } else {
                    validity.push(false);
                    starts.push(O::default());
                }
            }
            None => {
                validity.push(false);
                starts.push(O::default());
            }
        };
        Result::Ok(length)
    });
    let offsets = std::iter::once(Ok(O::default())).chain(offsets);
    let offsets = unsafe { Buffer::try_from_trusted_len_iter(offsets) }?;
    let starts: Buffer<O> = starts.into();

    let buffer = take_values(length, starts.as_slice(), offsets.as_slice(), values_values)?;

    Ok((offsets, buffer, validity.into()))
}
