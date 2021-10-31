use std::convert::TryFrom;

use chrono::Datelike;

use crate::{array::*, buffer::Buffer, datatypes::DataType, types::NativeType};
use crate::{
    error::{ArrowError, Result},
    temporal_conversions::{
        utf8_to_naive_timestamp_ns as utf8_to_naive_timestamp_ns_,
        utf8_to_timestamp_ns as utf8_to_timestamp_ns_, EPOCH_DAYS_FROM_CE,
    },
};

use super::CastOptions;

const RFC3339: &str = "%Y-%m-%dT%H:%M:%S%.f%:z";

/// Casts a [`Utf8Array`] to a [`PrimitiveArray`], making any uncastable value a Null.
pub fn utf8_to_primitive<O: Offset, T>(
    from: &Utf8Array<O>,
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

    let iter = from
        .iter()
        .map(|x| x.and_then::<T, _>(|x| parse_fn(x.as_bytes())));

    PrimitiveArray::<T>::from_trusted_len_iter(iter).to(to.clone())
}

pub(super) fn utf8_to_primitive_dyn<O: Offset, T>(
    from: &dyn Array,
    to: &DataType,
    options: CastOptions,
) -> Result<Box<dyn Array>>
where
    T: NativeType + lexical_core::FromLexical,
{
    let from = from.as_any().downcast_ref().unwrap();
    Ok(Box::new(utf8_to_primitive::<O, T>(from, to, options)))
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
    PrimitiveArray::<i32>::from_trusted_len_iter(iter).to(DataType::Date32)
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
    PrimitiveArray::<i64>::from_trusted_len_iter(iter).to(DataType::Date64)
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
    let mut array = MutableDictionaryArray::<K, MutableUtf8Array<O>>::new();
    array.try_extend(from.iter())?;

    Ok(array.into())
}

pub(super) fn utf8_to_naive_timestamp_ns_dyn<O: Offset>(
    from: &dyn Array,
) -> Result<Box<dyn Array>> {
    let from = from.as_any().downcast_ref().unwrap();
    Ok(Box::new(utf8_to_naive_timestamp_ns::<O>(from)))
}

/// [`crate::temporal_conversions::utf8_to_timestamp_ns`] applied for RFC3339 formatting
pub fn utf8_to_naive_timestamp_ns<O: Offset>(from: &Utf8Array<O>) -> PrimitiveArray<i64> {
    utf8_to_naive_timestamp_ns_(from, RFC3339)
}

pub(super) fn utf8_to_timestamp_ns_dyn<O: Offset>(
    from: &dyn Array,
    timezone: String,
) -> Result<Box<dyn Array>> {
    let from = from.as_any().downcast_ref().unwrap();
    utf8_to_timestamp_ns::<O>(from, timezone)
        .map(Box::new)
        .map(|x| x as Box<dyn Array>)
}

/// [`crate::temporal_conversions::utf8_to_timestamp_ns`] applied for RFC3339 formatting
pub fn utf8_to_timestamp_ns<O: Offset>(
    from: &Utf8Array<O>,
    timezone: String,
) -> Result<PrimitiveArray<i64>> {
    utf8_to_timestamp_ns_(from, RFC3339, timezone)
}

/// Conversion of utf8
pub fn utf8_to_large_utf8(from: &Utf8Array<i32>) -> Utf8Array<i64> {
    let data_type = Utf8Array::<i64>::default_data_type();
    let values = from.values().clone();
    let offsets = from.offsets().iter().map(|x| *x as i64);
    let offsets = Buffer::from_trusted_len_iter(offsets);
    unsafe {
        Utf8Array::<i64>::from_data_unchecked(data_type, offsets, values, from.validity().cloned())
    }
}

/// Conversion of utf8
pub fn utf8_large_to_utf8(from: &Utf8Array<i64>) -> Result<Utf8Array<i32>> {
    let data_type = Utf8Array::<i32>::default_data_type();
    let values = from.values().clone();
    let _ =
        i32::try_from(*from.offsets().last().unwrap()).map_err(ArrowError::from_external_error)?;

    let offsets = from.offsets().iter().map(|x| *x as i32);
    let offsets = Buffer::from_trusted_len_iter(offsets);
    Ok(unsafe {
        Utf8Array::<i32>::from_data_unchecked(data_type, offsets, values, from.validity().cloned())
    })
}
