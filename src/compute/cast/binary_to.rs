use std::convert::TryFrom;

use crate::error::{ArrowError, Result};
use crate::{array::*, buffer::Buffer, datatypes::DataType, types::NativeType};

use super::CastOptions;

/// Conversion of binary
pub fn binary_to_large_binary(from: &BinaryArray<i32>, to_data_type: DataType) -> BinaryArray<i64> {
    let values = from.values().clone();
    let offsets = from.offsets().iter().map(|x| *x as i64);
    let offsets = Buffer::from_trusted_len_iter(offsets);
    BinaryArray::<i64>::from_data(to_data_type, offsets, values, from.validity().cloned())
}

/// Conversion of binary
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
        from.validity().cloned(),
    ))
}

/// Casts a [`BinaryArray`] to a [`PrimitiveArray`], making any uncastable value a Null.
pub fn binary_to_primitive<O: Offset, T>(
    from: &BinaryArray<O>,
    to: &DataType,
    options: CastOptions,
) -> PrimitiveArray<T>
where
    T: NativeType + lexical_core::FromLexical,
{
    let parse_fn = if !options.partial {
        |x| lexical_core::parse(x).ok()
    } else {
        |x| lexical_core::parse_partial(x).ok().map(|x| x.0)
    };

    let iter = from.iter().map(|x| x.and_then::<T, _>(parse_fn));

    PrimitiveArray::<T>::from_trusted_len_iter(iter).to(to.clone())
}

pub(super) fn binary_to_primitive_dyn<O: Offset, T>(
    from: &dyn Array,
    to: &DataType,
    options: CastOptions,
) -> Result<Box<dyn Array>>
where
    T: NativeType + lexical_core::FromLexical,
{
    let from = from.as_any().downcast_ref().unwrap();
    Ok(Box::new(binary_to_primitive::<O, T>(from, to, options)))
}

/// Cast [`BinaryArray`] to [`DictionaryArray`], also known as packing.
/// # Errors
/// This function errors if the maximum key is smaller than the number of distinct elements
/// in the array.
pub fn binary_to_dictionary<O: Offset, K: DictionaryKey>(
    from: &BinaryArray<O>,
) -> Result<DictionaryArray<K>> {
    let mut array = MutableDictionaryArray::<K, MutableBinaryArray<O>>::new();
    array.try_extend(from.iter())?;

    Ok(array.into())
}

pub(super) fn binary_to_dictionary_dyn<O: Offset, K: DictionaryKey>(
    from: &dyn Array,
) -> Result<Box<dyn Array>> {
    let values = from.as_any().downcast_ref().unwrap();
    binary_to_dictionary::<O, K>(values).map(|x| Box::new(x) as Box<dyn Array>)
}
