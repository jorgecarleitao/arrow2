mod basic;
mod dictionary;
mod nested;
mod utils;

pub use dictionary::DictIter;
pub use utils::read_item;

use std::sync::Arc;

use crate::{array::Array, datatypes::DataType};

use super::{nested_utils::*, DataPages};

pub use basic::Iter;
use nested::ArrayIterator;

/// Converts [`DataPages`] to an [`Iterator`] of [`Array`]
pub fn iter_to_arrays_nested<'a, I, T, P, G, F>(
    iter: I,
    init: InitNested,
    data_type: DataType,
    chunk_size: usize,
    op1: G,
    op2: F,
) -> NestedArrayIter<'a>
where
    I: 'a + DataPages,
    T: crate::types::NativeType,
    P: parquet2::types::NativeType,
    G: 'a + Copy + Send + Sync + for<'b> Fn(&'b [u8]) -> P,
    F: 'a + Copy + Send + Sync + Fn(P) -> T,
{
    Box::new(
        ArrayIterator::<T, I, P, G, F>::new(iter, init, data_type, chunk_size, op1, op2).map(|x| {
            x.map(|(mut nested, array)| {
                let _ = nested.nested.pop().unwrap(); // the primitive
                let values = Arc::new(array) as Arc<dyn Array>;
                (nested, values)
            })
        }),
    )
}
