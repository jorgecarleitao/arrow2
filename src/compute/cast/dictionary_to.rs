use super::{cast, primitive_to::cast_typed_primitive};
use crate::{
    array::{Array, DictionaryKey},
    datatypes::DataType,
};
use crate::{
    array::{DictionaryArray, PrimitiveArray},
    compute::take::take,
    error::{ArrowError, Result},
};

macro_rules! key_cast {
    ($keys:expr, $values:expr, $array:expr, $to_keys_type:expr, $to_type:ty) => {{
        let cast_keys = cast_typed_primitive::<_, $to_type>($keys, $to_keys_type);

        // Failure to cast keys (because they don't fit in the
        // target type) results in NULL values;
        if cast_keys.null_count() > $keys.null_count() {
            return Err(ArrowError::DictionaryKeyOverflowError);
        }
        Ok(Box::new(DictionaryArray::<$to_type>::from_data(
            cast_keys, $values,
        )))
    }};
}

/// Attempts to cast an `ArrayDictionary` with keys type `K` into
/// `to_type`, for supported types.
pub fn dictionary_cast<K: DictionaryKey>(
    array: &dyn Array,
    to_type: &DataType,
) -> Result<Box<dyn Array>> {
    let array = array.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();
    let keys = array.keys();
    let values = array.values();

    match to_type {
        DataType::Dictionary(to_keys_type, to_values_type) => {
            let values = cast(values.as_ref(), to_values_type)?.into();

            // create the appropriate array type
            match to_keys_type.as_ref() {
                DataType::Int8 => key_cast!(keys, values, array, to_keys_type, i8),
                DataType::Int16 => key_cast!(keys, values, array, to_keys_type, i16),
                DataType::Int32 => key_cast!(keys, values, array, to_keys_type, i32),
                DataType::Int64 => key_cast!(keys, values, array, to_keys_type, i64),
                DataType::UInt8 => key_cast!(keys, values, array, to_keys_type, u8),
                DataType::UInt16 => key_cast!(keys, values, array, to_keys_type, u16),
                DataType::UInt32 => key_cast!(keys, values, array, to_keys_type, u32),
                DataType::UInt64 => key_cast!(keys, values, array, to_keys_type, u64),
                _ => unreachable!(),
            }
        }
        _ => unpack_dictionary::<K>(keys, values.as_ref(), to_type),
    }
}

// Unpack a dictionary where the keys are of type <K> into a flattened array of type to_type
fn unpack_dictionary<K>(
    keys: &PrimitiveArray<K>,
    values: &dyn Array,
    to_type: &DataType,
) -> Result<Box<dyn Array>>
where
    K: DictionaryKey,
{
    // attempt to cast the dict values to the target type
    // use the take kernel to expand out the dictionary
    let values = cast(values, to_type)?;

    // take requires first casting i32
    let indices = cast_typed_primitive::<_, i32>(keys, &DataType::UInt32);

    take(values.as_ref(), &indices, None)
}
