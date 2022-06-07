mod basic;
mod dictionary;
mod nested;
mod utils;

use std::sync::Arc;

use crate::{
    array::{Array, Offset},
    datatypes::DataType,
};

use self::basic::TraitBinaryArray;
use self::nested::ArrayIterator;
use super::{
    nested_utils::{InitNested, NestedArrayIter},
    DataPages,
};

pub use basic::Iter;
pub use dictionary::DictIter;

/// Converts [`DataPages`] to an [`Iterator`] of [`Array`]
pub fn iter_to_arrays_nested<'a, O, A, I>(
    iter: I,
    init: Vec<InitNested>,
    data_type: DataType,
    chunk_size: Option<usize>,
) -> NestedArrayIter<'a>
where
    I: 'a + DataPages,
    A: TraitBinaryArray<O>,
    O: Offset,
{
    Box::new(
        ArrayIterator::<O, A, I>::new(iter, init, data_type, chunk_size).map(|x| {
            x.map(|(mut nested, array)| {
                let _ = nested.nested.pop().unwrap(); // the primitive
                let values = Arc::new(array) as Arc<dyn Array>;
                (nested, values)
            })
        }),
    )
}
