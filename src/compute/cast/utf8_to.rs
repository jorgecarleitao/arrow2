use std::convert::TryFrom;

use chrono::Datelike;

use crate::{
    array::*,
    buffer::Buffer,
    datatypes::{DataType, TimeUnit},
    types::NativeType,
};
use crate::{
    error::{ArrowError, Result},
    temporal_conversions::EPOCH_DAYS_FROM_CE,
};

use super::utf8_to_timestamp_ns_scalar;

/// Casts a [`Utf8Array`] to a [`PrimitiveArray`], making any uncastable value a Null.
pub fn utf8_to_primitive<O: Offset, T>(from: &Utf8Array<O>, to: &DataType) -> PrimitiveArray<T>
where
    T: NativeType + lexical_core::FromLexical,
{
    let iter = from
        .iter()
        .map(|x| x.and_then::<T, _>(|x| lexical_core::parse(x.as_bytes()).ok()));

    Primitive::<T>::from_trusted_len_iter(iter).to(to.clone())
}

pub(super) fn utf8_to_primitive_dyn<O: Offset, T>(
    from: &dyn Array,
    to: &DataType,
) -> Result<Box<dyn Array>>
where
    T: NativeType + lexical_core::FromLexical,
{
    let from = from.as_any().downcast_ref().unwrap();
    Ok(Box::new(utf8_to_primitive::<O, T>(from, to)))
}

/// Casts a [`Utf8Array`] to a Date32 primitive, making any uncastable value a Null.
pub fn utf8_to_date32<O: Offset>(from: &Utf8Array<O>) -> PrimitiveArray<i32> {
    let iter = from.iter().map(|x| {
        x.and_then(|x| {
            x.parse::<chrono::NaiveDate>()
                .ok()
                .map(|x| x.num_days_from_ce() - EPOCH_DAYS_FROM_CE)
        })
    });
    Primitive::<i32>::from_trusted_len_iter(iter).to(DataType::Date32)
}

pub(super) fn utf8_to_date32_dyn<O: Offset>(from: &dyn Array) -> Result<Box<dyn Array>> {
    let from = from.as_any().downcast_ref().unwrap();
    Ok(Box::new(utf8_to_date32::<O>(from)))
}

/// Casts a [`Utf8Array`] to a Date64 primitive, making any uncastable value a Null.
pub fn utf8_to_date64<O: Offset>(from: &Utf8Array<O>) -> PrimitiveArray<i64> {
    let iter = from.iter().map(|x| {
        x.and_then(|x| {
            x.parse::<chrono::NaiveDateTime>()
                .ok()
                .map(|x| x.timestamp_millis())
        })
    });
    Primitive::<i64>::from_trusted_len_iter(iter).to(DataType::Date64)
}

pub(super) fn utf8_to_date64_dyn<O: Offset>(from: &dyn Array) -> Result<Box<dyn Array>> {
    let from = from.as_any().downcast_ref().unwrap();
    Ok(Box::new(utf8_to_date64::<O>(from)))
}

pub(super) fn utf8_to_dictionary_dyn<O: Offset, K: DictionaryKey>(
    from: &dyn Array,
) -> Result<Box<dyn Array>> {
    let values = from.as_any().downcast_ref().unwrap();
    utf8_to_dictionary::<O, K>(values).map(|x| Box::new(x) as Box<dyn Array>)
}

/// Cast [`Utf8Array`] to [`DictionaryArray`], also known as packing.
/// # Errors
/// This function errors if the maximum key is smaller than the number of distinct elements
/// in the array.
pub fn utf8_to_dictionary<O: Offset, K: DictionaryKey>(
    from: &Utf8Array<O>,
) -> Result<DictionaryArray<K>> {
    let mut primitive = DictionaryPrimitive::<K, _, _>::new(Utf8Primitive::<O>::new());
    primitive.try_extend(from)?;

    Ok(primitive.into())
}

pub(super) fn utf8_to_timestamp_ns_dyn<O: Offset>(from: &dyn Array) -> Result<Box<dyn Array>> {
    let from = from.as_any().downcast_ref().unwrap();
    Ok(Box::new(utf8_to_timestamp_ns::<O>(from)))
}

/// The array version of [`utf8_to_timestamp_ns_scalar`].
pub fn utf8_to_timestamp_ns<O: Offset>(from: &Utf8Array<O>) -> PrimitiveArray<i64> {
    let iter = from
        .iter()
        .map(|x| x.and_then(|x| utf8_to_timestamp_ns_scalar(x).ok()));
    Primitive::<i64>::from_trusted_len_iter(iter)
        .to(DataType::Timestamp(TimeUnit::Nanosecond, None))
}

pub fn utf8_to_large_utf8(from: &Utf8Array<i32>) -> Utf8Array<i64> {
    let values = from.values().clone();
    let offsets = from.offsets().iter().map(|x| *x as i64);
    let offsets = Buffer::from_trusted_len_iter(offsets);
    unsafe { Utf8Array::<i64>::from_data_unchecked(offsets, values, from.validity().clone()) }
}

pub fn utf8_large_to_utf8(from: &Utf8Array<i64>) -> Result<Utf8Array<i32>> {
    let values = from.values().clone();
    let _ =
        i32::try_from(*from.offsets().last().unwrap()).map_err(ArrowError::from_external_error)?;

    let offsets = from.offsets().iter().map(|x| *x as i32);
    let offsets = Buffer::from_trusted_len_iter(offsets);
    Ok(unsafe { Utf8Array::<i32>::from_data_unchecked(offsets, values, from.validity().clone()) })
}
