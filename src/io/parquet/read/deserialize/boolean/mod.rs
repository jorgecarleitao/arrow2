mod basic;
mod nested;

use std::sync::Arc;

use crate::array::Array;

use self::nested::ArrayIterator;
use super::{
    nested_utils::{InitNested, NestedArrayIter},
    DataPages,
};

pub use self::basic::Iter;

/// Converts [`DataPages`] to an [`Iterator`] of [`Array`]
pub fn iter_to_arrays_nested<'a, I: 'a>(
    iter: I,
    init: Vec<InitNested>,
    chunk_size: usize,
) -> NestedArrayIter<'a>
where
    I: DataPages,
{
    Box::new(ArrayIterator::new(iter, init, chunk_size).map(|x| {
        x.map(|(mut nested, array)| {
            let _ = nested.nested.pop().unwrap(); // the primitive
            let values = Arc::new(array) as Arc<dyn Array>;
            (nested, values)
        })
    }))
}
