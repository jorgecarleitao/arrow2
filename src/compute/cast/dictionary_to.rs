use super::{primitive_as_primitive, primitive_to_primitive, CastOptions};
use crate::{
    array::{Array, DictionaryArray, DictionaryKey, PrimitiveArray},
    compute::{cast::cast, take::take},
    datatypes::DataType,
    error::{Error, Result},
};

macro_rules! key_cast {
    ($keys:expr, $values:expr, $array:expr, $to_keys_type:expr, $to_type:ty) => {{
        let cast_keys = primitive_to_primitive::<_, $to_type>($keys, $to_keys_type);

        // Failure to cast keys (because they don't fit in the
        // target type) results in NULL values;
        if cast_keys.null_count() > $keys.null_count() {
            return Err(Error::Overflow);
        }
        Ok(Box::new(DictionaryArray::<$to_type>::from_data(
            cast_keys, $values,
        )))
    }};
}

/// Casts a [`DictionaryArray`] to a new [`DictionaryArray`] by keeping the
/// keys and casting the values to `values_type`.
/// # Errors
/// This function errors if the values are not castable to `values_type`
pub fn dictionary_to_dictionary_values<K: DictionaryKey>(
    from: &DictionaryArray<K>,
    values_type: &DataType,
) -> Result<DictionaryArray<K>> {
    let keys = from.keys();
    let values = from.values();

    let values = cast(values.as_ref(), values_type, CastOptions::default())?;
    Ok(DictionaryArray::from_data(keys.clone(), values))
}

/// Similar to dictionary_to_dictionary_values, but overflowing cast is wrapped
pub fn wrapping_dictionary_to_dictionary_values<K: DictionaryKey>(
    from: &DictionaryArray<K>,
    values_type: &DataType,
) -> Result<DictionaryArray<K>> {
    let keys = from.keys();
    let values = from.values();

    let values = cast(
        values.as_ref(),
        values_type,
        CastOptions {
            wrapped: true,
            partial: false,
        },
    )?;
    Ok(DictionaryArray::from_data(keys.clone(), values))
}

/// Casts a [`DictionaryArray`] to a new [`DictionaryArray`] backed by a
/// different physical type of the keys, while keeping the values equal.
/// # Errors
/// Errors if any of the old keys' values is larger than the maximum value
/// supported by the new physical type.
pub fn dictionary_to_dictionary_keys<K1, K2>(
    from: &DictionaryArray<K1>,
) -> Result<DictionaryArray<K2>>
where
    K1: DictionaryKey,
    K2: DictionaryKey,
{
    let keys = from.keys();
    let values = from.values();

    let casted_keys = primitive_to_primitive::<K1, K2>(keys, &K2::PRIMITIVE.into());

    if casted_keys.null_count() > keys.null_count() {
        Err(Error::Overflow)
    } else {
        Ok(DictionaryArray::from_data(casted_keys, values.clone()))
    }
}

/// Similar to dictionary_to_dictionary_keys, but overflowing cast is wrapped
pub fn wrapping_dictionary_to_dictionary_keys<K1, K2>(
    from: &DictionaryArray<K1>,
) -> Result<DictionaryArray<K2>>
where
    K1: DictionaryKey + num_traits::AsPrimitive<K2>,
    K2: DictionaryKey,
{
    let keys = from.keys();
    let values = from.values();

    let casted_keys = primitive_as_primitive::<K1, K2>(keys, &K2::PRIMITIVE.into());

    if casted_keys.null_count() > keys.null_count() {
        Err(Error::Overflow)
    } else {
        Ok(DictionaryArray::from_data(casted_keys, values.clone()))
    }
}

pub(super) fn dictionary_cast_dyn<K: DictionaryKey>(
    array: &dyn Array,
    to_type: &DataType,
    options: CastOptions,
) -> Result<Box<dyn Array>> {
    let array = array.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();
    let keys = array.keys();
    let values = array.values();

    match to_type {
        DataType::Dictionary(to_keys_type, to_values_type, _) => {
            let values = cast(values.as_ref(), to_values_type, options)?;

            // create the appropriate array type
            let data_type = (*to_keys_type).into();
            match_integer_type!(to_keys_type, |$T| {
                key_cast!(keys, values, array, &data_type, $T)
            })
        }
        _ => unpack_dictionary::<K>(keys, values.as_ref(), to_type, options),
    }
}

// Unpack the dictionary
fn unpack_dictionary<K>(
    keys: &PrimitiveArray<K>,
    values: &dyn Array,
    to_type: &DataType,
    options: CastOptions,
) -> Result<Box<dyn Array>>
where
    K: DictionaryKey,
{
    // attempt to cast the dict values to the target type
    // use the take kernel to expand out the dictionary
    let values = cast(values, to_type, options)?;

    // take requires first casting i32
    let indices = primitive_to_primitive::<_, i32>(keys, &DataType::Int32);

    take(values.as_ref(), &indices)
}

/// Casts a [`DictionaryArray`] to its values' [`DataType`], also known as unpacking.
/// The resulting array has the same length.
pub fn dictionary_to_values<K>(from: &DictionaryArray<K>) -> Box<dyn Array>
where
    K: DictionaryKey,
{
    // take requires first casting i64
    let indices = primitive_to_primitive::<_, i64>(from.keys(), &DataType::Int64);

    // unwrap: The dictionary guarantees that the keys are not out-of-bounds.
    take(from.values().as_ref(), &indices).unwrap()
}
