mod basic;
mod dictionary;
mod nested;
mod utils;

pub use dictionary::iter_to_arrays as iter_to_dict_arrays;
pub use utils::read_item;

use std::sync::Arc;

use super::{nested_utils::*, DataPages};
use crate::{
    array::Array,
    datatypes::{DataType, Field},
    error::Result,
};

use basic::PrimitiveArrayIterator;
use nested::ArrayIterator;

/// Converts [`DataPages`] to an [`Iterator`] of [`Array`]
pub fn iter_to_arrays<'a, I, T, P, G, F>(
    iter: I,
    data_type: DataType,
    chunk_size: usize,
    op1: G,
    op2: F,
) -> Box<dyn Iterator<Item = Result<Arc<dyn Array>>> + 'a>
where
    I: 'a + DataPages,
    T: crate::types::NativeType,
    P: parquet2::types::NativeType,
    G: 'a + Copy + for<'b> Fn(&'b [u8]) -> P,
    F: 'a + Copy + Fn(P) -> T,
{
    Box::new(
        PrimitiveArrayIterator::<T, I, P, G, F>::new(iter, data_type, chunk_size, op1, op2)
            .map(|x| x.map(|x| Arc::new(x) as Arc<dyn Array>)),
    )
}

/// Converts [`DataPages`] to an [`Iterator`] of [`Array`]
pub fn iter_to_arrays_nested<'a, I, T, P, G, F>(
    iter: I,
    field: Field,
    data_type: DataType,
    chunk_size: usize,
    op1: G,
    op2: F,
) -> Box<dyn Iterator<Item = Result<(NestedState, Arc<dyn Array>)>> + 'a>
where
    I: 'a + DataPages,
    T: crate::types::NativeType,
    P: parquet2::types::NativeType,
    G: 'a + Copy + for<'b> Fn(&'b [u8]) -> P,
    F: 'a + Copy + Fn(P) -> T,
{
    Box::new(
        ArrayIterator::<T, I, P, G, F>::new(iter, field, data_type, chunk_size, op1, op2).map(
            |x| {
                x.map(|(nested, array)| {
                    let values = Arc::new(array) as Arc<dyn Array>;
                    (nested, values)
                })
            },
        ),
    )
}
