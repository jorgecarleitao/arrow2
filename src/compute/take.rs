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

//! Defines take kernel for [Array]

use crate::{
    buffer::MutableBitmap,
    error::{ArrowError, Result},
};

use crate::{
    array::{Array, Offset, PrimitiveArray},
    buffer::{types::NativeType, Bitmap, Buffer, MutableBuffer},
    datatypes::DataType,
};

macro_rules! downcast_take {
    ($type: ty, $values: expr, $indices: expr) => {{
        let values = $values
            .as_any()
            .downcast_ref::<PrimitiveArray<$type>>()
            .expect("Unable to downcast to a primitive array");
        Ok(Box::new(take_primitive::<$type, _>(&values, $indices)?))
    }};
}

pub fn take<O: Offset>(
    values: &dyn Array,
    indices: &PrimitiveArray<O>,
    options: Option<TakeOptions>,
) -> Result<Box<dyn Array>> {
    take_impl(values, indices, options)
}

fn take_impl<O: Offset>(
    values: &dyn Array,
    indices: &PrimitiveArray<O>,
    options: Option<TakeOptions>,
) -> Result<Box<dyn Array>> {
    match values.data_type() {
        DataType::Int8 => downcast_take!(i8, values, indices),
        DataType::Int16 => downcast_take!(i16, values, indices),
        DataType::Int32 => downcast_take!(i32, values, indices),
        DataType::Int64 => downcast_take!(i64, values, indices),
        DataType::UInt8 => downcast_take!(u8, values, indices),
        DataType::UInt16 => downcast_take!(u16, values, indices),
        DataType::UInt32 => downcast_take!(u32, values, indices),
        DataType::UInt64 => downcast_take!(u64, values, indices),
        DataType::Float32 => downcast_take!(f32, values, indices),
        DataType::Float64 => downcast_take!(f64, values, indices),
        t => unimplemented!("Take not supported for data type {:?}", t),
    }
}

/// Options that define how `take` should behave
#[derive(Clone, Debug)]
pub struct TakeOptions {
    /// Perform bounds check before taking indices from values.
    /// If enabled, an `ArrowError` is returned if the indices are out of bounds.
    /// If not enabled, and indices exceed bounds, the kernel will panic.
    pub check_bounds: bool,
}

impl Default for TakeOptions {
    fn default() -> Self {
        Self {
            check_bounds: false,
        }
    }
}

#[inline(always)]
fn maybe_usize<I: Offset>(index: I) -> Result<usize> {
    index
        .to_usize()
        .ok_or_else(|| ArrowError::ComputeError("Cast to usize failed".to_string()))
}

// take implementation when neither values nor indices contain nulls
fn take_no_nulls<T: NativeType, I: Offset>(
    values: &[T],
    indices: &[I],
) -> Result<(Buffer<T>, Option<Bitmap>)> {
    let values = indices
        .iter()
        .map(|index| Result::Ok(values[maybe_usize::<I>(*index)?]));
    // Soundness: `slice.map` is `TrustedLen`.
    let buffer = unsafe { MutableBuffer::try_from_trusted_len_iter(values)? };

    Ok((buffer.into(), None))
}

// take implementation when only values contain nulls
fn take_values_nulls<T: NativeType, I: Offset>(
    values: &PrimitiveArray<T>,
    indices: &[I],
) -> Result<(Buffer<T>, Option<Bitmap>)> {
    let mut null = MutableBitmap::with_capacity(indices.len());

    let null_values = values.nulls().as_ref().unwrap();

    let values_values = values.values();

    let values = indices.iter().map(|index| {
        let index = maybe_usize::<I>(*index)?;
        if null_values.get_bit(index) {
            null.push(true);
        } else {
            null.push(false);
        }
        Result::Ok(values_values[index])
    });
    // Soundness: `slice.map` is `TrustedLen`.
    let buffer = unsafe { MutableBuffer::try_from_trusted_len_iter(values)? };

    Ok((buffer.into(), null.into()))
}

// take implementation when only indices contain nulls
fn take_indices_nulls<T: NativeType, I: Offset>(
    values: &[T],
    indices: &PrimitiveArray<I>,
) -> Result<(Buffer<T>, Option<Bitmap>)> {
    let null_indices = indices.nulls().as_ref().unwrap();

    let values = indices.values().iter().map(|index| {
        let index = maybe_usize::<I>(*index)?;
        Result::Ok(match values.get(index) {
            Some(value) => *value,
            None => {
                if null_indices.get_bit(index) {
                    panic!("Out-of-bounds index {}", index)
                } else {
                    T::default()
                }
            }
        })
    });

    // Soundness: `slice.map` is `TrustedLen`.
    let buffer = unsafe { MutableBuffer::try_from_trusted_len_iter(values)? };

    Ok((buffer.into(), indices.nulls().clone()))
}

// take implementation when both values and indices contain nulls
fn take_values_indices_nulls<T: NativeType, I: Offset>(
    values: &PrimitiveArray<T>,
    indices: &PrimitiveArray<I>,
) -> Result<(Buffer<T>, Option<Bitmap>)> {
    let mut bitmap = MutableBitmap::with_capacity(indices.len());

    let null_values = values.nulls().as_ref().unwrap();

    let values_values = values.values();
    let values = indices.iter().map(|index| match index {
        Some(index) => {
            let index = maybe_usize::<I>(index)?;
            bitmap.push(null_values.get_bit(index));
            Result::Ok(values_values[index])
        }
        None => {
            bitmap.push(false);
            Ok(T::default())
        }
    });
    // Soundness: `slice.map` is `TrustedLen`.
    let buffer = unsafe { MutableBuffer::try_from_trusted_len_iter(values)? };
    Ok((buffer.into(), bitmap.into()))
}

/// `take` implementation for all primitive arrays
///
/// This checks if an `indices` slot is populated, and gets the value from `values`
///  as the populated index.
/// If the `indices` slot is null, a null value is returned.
/// For example, given:
///     values:  [1, 2, 3, null, 5]
///     indices: [0, null, 4, 3]
/// The result is: [1 (slot 0), null (null slot), 5 (slot 4), null (slot 3)]
fn take_primitive<T: NativeType, I: Offset>(
    values: &PrimitiveArray<T>,
    indices: &PrimitiveArray<I>,
) -> Result<PrimitiveArray<T>> {
    let indices_has_nulls = indices.null_count() > 0;
    let values_has_nulls = values.null_count() > 0;
    // note: this function should only panic when "an index is not null and out of bounds".
    // if the index is null, its value is undefined and therefore we should not read from it.

    let (buffer, nulls) = match (values_has_nulls, indices_has_nulls) {
        (false, false) => {
            // * no nulls
            // * all `indices.values()` are valid
            take_no_nulls::<T, I>(values.values(), indices.values())?
        }
        (true, false) => {
            // * nulls come from `values` alone
            // * all `indices.values()` are valid
            take_values_nulls::<T, I>(values, indices.values())?
        }
        (false, true) => {
            // in this branch it is unsound to read and use `index.values()`,
            // as doing so is UB when they come from a null slot.
            take_indices_nulls::<T, I>(values.values(), indices)?
        }
        (true, true) => {
            // in this branch it is unsound to read and use `index.values()`,
            // as doing so is UB when they come from a null slot.
            take_values_indices_nulls::<T, I>(values, indices)?
        }
    };

    Ok(PrimitiveArray::<T>::from_data(
        values.data_type().clone(),
        buffer,
        nulls,
    ))
}

#[cfg(test)]
mod tests {
    use crate::{
        array::Primitive,
        datatypes::{Int8Type, PrimitiveType},
    };

    use super::*;

    fn test_take_primitive_arrays<T>(
        data: &[Option<T::Native>],
        index: &PrimitiveArray<i32>,
        options: Option<TakeOptions>,
        expected_data: &[Option<T::Native>],
    ) -> Result<()>
    where
        T: PrimitiveType,
    {
        let output = Primitive::<T::Native>::from(data).to(T::DATA_TYPE);
        let expected = Primitive::<T::Native>::from(expected_data).to(T::DATA_TYPE);
        let output = take(&output, index, options)?;
        assert_eq!(output.as_ref(), &expected);
        Ok(())
    }

    #[test]
    fn test_take_primitive_non_null_indices() {
        let index = Primitive::<i32>::from_slice(&[0, 5, 3, 1, 4, 2]).to(DataType::Int32);
        test_take_primitive_arrays::<Int8Type>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            &index,
            None,
            &[None, None, Some(2), Some(3), Some(3), Some(5)],
        )
        .unwrap();
    }

    #[test]
    fn test_take_primitive_non_null_values() {
        let index =
            Primitive::<i32>::from(&[Some(3), None, Some(1), Some(3), Some(2)]).to(DataType::Int32);
        test_take_primitive_arrays::<Int8Type>(
            &[Some(0), Some(1), Some(2), Some(3), Some(4)],
            &index,
            None,
            &[Some(3), None, Some(1), Some(3), Some(2)],
        )
        .unwrap();
    }
}
