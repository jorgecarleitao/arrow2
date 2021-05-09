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
    array::{Array, BooleanArray, Offset, PrimitiveArray},
    bitmap::{Bitmap, MutableBitmap},
    error::Result,
};

use super::maybe_usize;

// take implementation when neither values nor indices contain nulls
fn take_no_validity<I: Offset>(values: &Bitmap, indices: &[I]) -> Result<(Bitmap, Option<Bitmap>)> {
    let values = indices
        .iter()
        .map(|index| Result::Ok(values.get_bit(maybe_usize::<I>(*index)?)));
    let buffer = Bitmap::try_from_trusted_len_iter(values)?;

    Ok((buffer, None))
}

// take implementation when only values contain nulls
fn take_values_validity<I: Offset>(
    values: &BooleanArray,
    indices: &[I],
) -> Result<(Bitmap, Option<Bitmap>)> {
    let mut validity = MutableBitmap::with_capacity(indices.len());

    let validity_values = values.validity().as_ref().unwrap();

    let values_values = values.values();

    let values = indices.iter().map(|index| {
        let index = maybe_usize::<I>(*index)?;
        if validity_values.get_bit(index) {
            validity.push(true);
        } else {
            validity.push(false);
        }
        Result::Ok(values_values.get_bit(index))
    });
    let buffer = Bitmap::try_from_trusted_len_iter(values)?;

    Ok((buffer, validity.into()))
}

// take implementation when only indices contain nulls
fn take_indices_validity<I: Offset>(
    values: &Bitmap,
    indices: &PrimitiveArray<I>,
) -> Result<(Bitmap, Option<Bitmap>)> {
    let null_indices = indices.validity().as_ref().unwrap();

    let values = indices.values().iter().map(|index| {
        let index = maybe_usize::<I>(*index)?;
        Result::Ok(match values.get(index) {
            Some(value) => value,
            None => {
                if null_indices.get_bit(index) {
                    panic!("Out-of-bounds index {}", index)
                } else {
                    false
                }
            }
        })
    });

    let buffer = Bitmap::try_from_trusted_len_iter(values)?;

    Ok((buffer, indices.validity().clone()))
}

// take implementation when both values and indices contain nulls
fn take_values_indices_validity<I: Offset>(
    values: &BooleanArray,
    indices: &PrimitiveArray<I>,
) -> Result<(Bitmap, Option<Bitmap>)> {
    let mut validity = MutableBitmap::with_capacity(indices.len());

    let values_validity = values.validity().as_ref().unwrap();

    let values_values = values.values();
    let values = indices.iter().map(|index| match index {
        Some(index) => {
            let index = maybe_usize::<I>(*index)?;
            validity.push(values_validity.get_bit(index));
            Result::Ok(values_values.get_bit(index))
        }
        None => {
            validity.push(false);
            Ok(false)
        }
    });
    let values = Bitmap::try_from_trusted_len_iter(values)?;
    Ok((values, validity.into()))
}

/// `take` implementation for boolean arrays
pub fn take<I: Offset>(values: &BooleanArray, indices: &PrimitiveArray<I>) -> Result<BooleanArray> {
    let indices_has_validity = indices.null_count() > 0;
    let values_has_validity = values.null_count() > 0;

    let (values, validity) = match (values_has_validity, indices_has_validity) {
        (false, false) => take_no_validity(values.values(), indices.values())?,
        (true, false) => take_values_validity(values, indices.values())?,
        (false, true) => take_indices_validity(values.values(), indices)?,
        (true, true) => take_values_indices_validity(values, indices)?,
    };

    Ok(BooleanArray::from_data(values, validity))
}

#[cfg(test)]
mod tests {
    use crate::array::Int32Array;

    use super::*;

    fn _all_cases() -> Vec<(Int32Array, BooleanArray, BooleanArray)> {
        vec![
            (
                Int32Array::from(&[Some(1), Some(0)]),
                BooleanArray::from(vec![Some(true), Some(false)]),
                BooleanArray::from(vec![Some(false), Some(true)]),
            ),
            (
                Int32Array::from(&[Some(1), None]),
                BooleanArray::from(vec![Some(true), Some(false)]),
                BooleanArray::from(vec![Some(false), None]),
            ),
            (
                Int32Array::from(&[Some(1), Some(0)]),
                BooleanArray::from(vec![None, Some(false)]),
                BooleanArray::from(vec![Some(false), None]),
            ),
            (
                Int32Array::from(&[Some(1), None, Some(0)]),
                BooleanArray::from(vec![None, Some(false)]),
                BooleanArray::from(vec![Some(false), None, None]),
            ),
        ]
    }

    #[test]
    fn all_cases() -> Result<()> {
        let cases = _all_cases();
        for (indices, input, expected) in cases {
            let output = take(&input, &indices)?;
            assert_eq!(expected, output);
        }
        Ok(())
    }
}
