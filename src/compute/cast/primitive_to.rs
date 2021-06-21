use std::hash::Hash;

use crate::{
    array::*,
    bitmap::Bitmap,
    compute::arity::unary,
    datatypes::{DataType, TimeUnit},
    temporal_conversions::*,
    types::NativeType,
};
use crate::{error::Result, util::lexical_to_string};

/// Returns a [`BooleanArray`] where every element is different from zero.
/// Validity is preserved.
pub fn primitive_to_boolean<T: NativeType>(from: &PrimitiveArray<T>) -> BooleanArray {
    let iter = from.values().iter().map(|v| *v != T::default());
    let values = Bitmap::from_trusted_len_iter(iter);

    BooleanArray::from_data(values, from.validity().clone())
}

pub(super) fn primitive_to_boolean_dyn<T>(from: &dyn Array) -> Result<Box<dyn Array>>
where
    T: NativeType,
{
    let from = from.as_any().downcast_ref().unwrap();
    Ok(Box::new(primitive_to_boolean::<T>(from)))
}

/// Returns a [`Utf8Array`] where every element is the utf8 representation of the number.
pub fn primitive_to_utf8<T: NativeType + lexical_core::ToLexical, O: Offset>(
    from: &PrimitiveArray<T>,
) -> Utf8Array<O> {
    let iter = from.iter().map(|x| x.map(|x| lexical_to_string(*x)));

    Utf8Array::from_trusted_len_iter(iter)
}

pub(super) fn primitive_to_utf8_dyn<T, O>(from: &dyn Array) -> Result<Box<dyn Array>>
where
    O: Offset,
    T: NativeType + lexical_core::ToLexical,
{
    let from = from.as_any().downcast_ref().unwrap();
    Ok(Box::new(primitive_to_utf8::<T, O>(from)))
}

pub(super) fn primitive_to_primitive_dyn<I, O>(
    from: &dyn Array,
    to_type: &DataType,
) -> Result<Box<dyn Array>>
where
    I: NativeType + num::NumCast,
    O: NativeType + num::NumCast,
{
    let from = from.as_any().downcast_ref::<PrimitiveArray<I>>().unwrap();
    Ok(Box::new(primitive_to_primitive::<I, O>(from, to_type)))
}

/// Cast [`PrimitiveArray`] to a [`PrimitiveArray`] of another physical type via numeric conversion.
pub fn primitive_to_primitive<I, O>(
    from: &PrimitiveArray<I>,
    to_type: &DataType,
) -> PrimitiveArray<O>
where
    I: NativeType + num::NumCast,
    O: NativeType + num::NumCast,
{
    let from = from.as_any().downcast_ref::<PrimitiveArray<I>>().unwrap();

    let iter = from
        .iter()
        .map(|v| v.and_then(|x| num::cast::cast::<I, O>(*x)));
    PrimitiveArray::<O>::from_trusted_len_iter(iter).to(to_type.clone())
}

/// Cast [`PrimitiveArray`] to a [`PrimitiveArray`] of the same physical type.
/// This is O(1).
pub fn primitive_to_same_primitive<T>(
    from: &PrimitiveArray<T>,
    to_type: &DataType,
) -> PrimitiveArray<T>
where
    T: NativeType,
{
    PrimitiveArray::<T>::from_data(
        to_type.clone(),
        from.values().clone(),
        from.validity().clone(),
    )
}

/// Cast [`PrimitiveArray`] to a [`PrimitiveArray`] of the same physical type.
/// This is O(1).
pub(super) fn primitive_to_same_primitive_dyn<T>(
    from: &dyn Array,
    to_type: &DataType,
) -> Result<Box<dyn Array>>
where
    T: NativeType,
{
    let from = from.as_any().downcast_ref().unwrap();
    Ok(Box::new(primitive_to_same_primitive::<T>(from, to_type)))
}

pub(super) fn primitive_to_dictionary_dyn<T: NativeType + Eq + Hash, K: DictionaryKey>(
    from: &dyn Array,
) -> Result<Box<dyn Array>> {
    let from = from.as_any().downcast_ref().unwrap();
    primitive_to_dictionary::<T, K>(from).map(|x| Box::new(x) as Box<dyn Array>)
}

/// Cast [`PrimitiveArray`] to [`DictionaryArray`]. Also known as packing.
/// # Errors
/// This function errors if the maximum key is smaller than the number of distinct elements
/// in the array.
pub fn primitive_to_dictionary<T: NativeType + Eq + Hash, K: DictionaryKey>(
    from: &PrimitiveArray<T>,
) -> Result<DictionaryArray<K>> {
    let iter = from.iter().map(|x| x.copied());
    let mut array = MutableDictionaryArray::<K, _>::from(MutablePrimitiveArray::<T>::from(
        from.data_type().clone(),
    ));
    array.try_extend(iter)?;

    Ok(array.into())
}

/// Get the time unit as a multiple of a second
const fn time_unit_multiple(unit: &TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => MILLISECONDS,
        TimeUnit::Microsecond => MICROSECONDS,
        TimeUnit::Nanosecond => NANOSECONDS,
    }
}

pub fn date32_to_date64(from: &PrimitiveArray<i32>) -> PrimitiveArray<i64> {
    unary(from, |x| x as i64 * MILLISECONDS_IN_DAY, DataType::Date64)
}

pub fn date64_to_date32(from: &PrimitiveArray<i64>) -> PrimitiveArray<i32> {
    unary(from, |x| (x / MILLISECONDS_IN_DAY) as i32, DataType::Date32)
}

pub fn time32s_to_time32ms(from: &PrimitiveArray<i32>) -> PrimitiveArray<i32> {
    unary(from, |x| x * 1000, DataType::Time32(TimeUnit::Millisecond))
}

pub fn time32ms_to_time32s(from: &PrimitiveArray<i32>) -> PrimitiveArray<i32> {
    unary(from, |x| x / 1000, DataType::Time32(TimeUnit::Second))
}

pub fn time64us_to_time64ns(from: &PrimitiveArray<i64>) -> PrimitiveArray<i64> {
    unary(from, |x| x * 1000, DataType::Time64(TimeUnit::Nanosecond))
}

pub fn time64ns_to_time64us(from: &PrimitiveArray<i64>) -> PrimitiveArray<i64> {
    unary(from, |x| x / 1000, DataType::Time64(TimeUnit::Microsecond))
}

pub fn timestamp_to_date64(
    from: &PrimitiveArray<i64>,
    from_unit: &TimeUnit,
) -> PrimitiveArray<i64> {
    let from_size = time_unit_multiple(from_unit);
    let to_size = MILLISECONDS;
    let to_type = DataType::Date64;

    // Scale time_array by (to_size / from_size) using a
    // single integer operation, but need to avoid integer
    // math rounding down to zero

    match to_size.cmp(&from_size) {
        std::cmp::Ordering::Less => unary(from, |x| (x / (from_size / to_size)), to_type),
        std::cmp::Ordering::Equal => primitive_to_same_primitive(from, &to_type),
        std::cmp::Ordering::Greater => unary(from, |x| (x * (to_size / from_size)), to_type),
    }
}

pub fn timestamp_to_date32(
    from: &PrimitiveArray<i64>,
    from_unit: &TimeUnit,
) -> PrimitiveArray<i32> {
    let from_size = time_unit_multiple(from_unit) * SECONDS_IN_DAY;
    unary(from, |x| (x / from_size) as i32, DataType::Date32)
}

pub fn time32_to_time64(
    from: &PrimitiveArray<i32>,
    from_unit: &TimeUnit,
    to_unit: &TimeUnit,
) -> PrimitiveArray<i64> {
    let from_size = time_unit_multiple(from_unit);
    let to_size = time_unit_multiple(to_unit);
    let divisor = to_size / from_size;
    unary(
        from,
        |x| (x as i64 * divisor),
        DataType::Time64(to_unit.clone()),
    )
}

pub fn time64_to_time32(
    from: &PrimitiveArray<i64>,
    from_unit: &TimeUnit,
    to_unit: &TimeUnit,
) -> PrimitiveArray<i32> {
    let from_size = time_unit_multiple(&from_unit);
    let to_size = time_unit_multiple(&to_unit);
    let divisor = from_size / to_size;
    unary(
        from,
        |x| (x as i64 / divisor) as i32,
        DataType::Time32(to_unit.clone()),
    )
}

pub fn timestamp_to_timestamp(
    from: &PrimitiveArray<i64>,
    from_unit: &TimeUnit,
    to_unit: &TimeUnit,
    tz: &Option<String>,
) -> PrimitiveArray<i64> {
    let from_size = time_unit_multiple(&from_unit);
    let to_size = time_unit_multiple(&to_unit);
    let to_type = DataType::Timestamp(to_unit.clone(), tz.clone());
    // we either divide or multiply, depending on size of each unit
    if from_size >= to_size {
        unary(from, |x| (x / (from_size / to_size)), to_type)
    } else {
        unary(from, |x| (x * (to_size / from_size)), to_type)
    }
}
