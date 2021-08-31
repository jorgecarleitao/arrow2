use std::convert::TryFrom;

use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};
use crate::{array::*, buffer::Buffer};

pub fn binary_to_large_binary(from: &BinaryArray<i32>, to_data_type: DataType) -> BinaryArray<i64> {
    let values = from.values().clone();
    let offsets = from.offsets().iter().map(|x| *x as i64);
    let offsets = Buffer::from_trusted_len_iter(offsets);
    BinaryArray::<i64>::from_data(to_data_type, offsets, values, from.validity().clone())
}

pub fn binary_large_to_binary(
    from: &BinaryArray<i64>,
    to_data_type: DataType,
) -> Result<BinaryArray<i32>> {
    let values = from.values().clone();
    let _ =
        i32::try_from(*from.offsets().last().unwrap()).map_err(ArrowError::from_external_error)?;

    let offsets = from.offsets().iter().map(|x| *x as i32);
    let offsets = Buffer::from_trusted_len_iter(offsets);
    Ok(BinaryArray::<i32>::from_data(
        to_data_type,
        offsets,
        values,
        from.validity().clone(),
    ))
}
