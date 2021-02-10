use crate::{
    array::{Array, BooleanArray, Primitive},
    buffer::NativeType,
    datatypes::DataType,
};
use crate::{
    array::{Offset, Utf8Array},
    error::Result,
};

/// Cast Boolean types to numeric
///
/// `false` returns 0 while `true` returns 1
pub fn cast_bool_to_numeric<T>(array: &dyn Array, to: &DataType) -> Result<Box<dyn Array>>
where
    T: NativeType + num::One,
{
    let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();

    let iter = array
        .iter()
        .map(|x| x.map(|x| if x { T::one() } else { T::default() }));

    // Soundness:
    //     The iterator is trustedLen
    let array = unsafe { Primitive::<T>::from_trusted_len_iter(iter) }.to(to.clone());

    Ok(Box::new(array))
}

/// Cast Boolean types to numeric
///
/// `false` returns 0 while `true` returns 1
pub fn cast_bool_to_utf8<O: Offset>(array: &dyn Array) -> Result<Box<dyn Array>> {
    let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();

    let iter = array.iter().map(|x| x.map(|x| if x { "1" } else { "0" }));

    // Soundness:
    //     The iterator is trustedLen
    let array = unsafe { Utf8Array::<O>::from_trusted_len_iter(iter) };

    Ok(Box::new(array))
}
