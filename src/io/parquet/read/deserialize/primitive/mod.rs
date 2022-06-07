mod basic;
mod dictionary;
mod nested;

pub use dictionary::DictIter;

use crate::datatypes::DataType;

use super::{nested_utils::*, DataPages};

pub use basic::Iter;
use nested::ArrayIterator;

/// Converts [`DataPages`] to an [`Iterator`] of [`Array`]
pub fn iter_to_arrays_nested<'a, I, T, P, F>(
    iter: I,
    init: Vec<InitNested>,
    data_type: DataType,
    chunk_size: Option<usize>,
    op: F,
) -> NestedArrayIter<'a>
where
    I: 'a + DataPages,
    T: crate::types::NativeType,
    P: parquet2::types::NativeType,
    F: 'a + Copy + Send + Sync + Fn(P) -> T,
{
    Box::new(
        ArrayIterator::<T, I, P, F>::new(iter, init, data_type, chunk_size, op).map(|x| {
            x.map(|(mut nested, array)| {
                let _ = nested.nested.pop().unwrap(); // the primitive
                (nested, array.arced())
            })
        }),
    )
}
