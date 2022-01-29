use std::sync::Arc;

use crate::{
    array::{Array, Offset},
    datatypes::DataType,
    error::Result,
};

mod basic;
mod dictionary;
mod nested;
mod utils;

pub use dictionary::iter_to_arrays as iter_to_dict_arrays;

use self::basic::TraitBinaryArray;

use super::DataPages;
use basic::BinaryArrayIterator;

/// Converts [`DataPages`] to an [`Iterator`] of [`Array`]
pub fn iter_to_arrays<'a, O, A, I>(
    iter: I,
    data_type: DataType,
    chunk_size: usize,
) -> Box<dyn Iterator<Item = Result<Arc<dyn Array>>> + 'a>
where
    I: 'a + DataPages,
    A: TraitBinaryArray<O>,
    O: Offset,
{
    Box::new(
        BinaryArrayIterator::<O, A, I>::new(iter, data_type, chunk_size)
            .map(|x| x.map(|x| Arc::new(x) as Arc<dyn Array>)),
    )
}
