mod basic;
mod nested;

use std::sync::Arc;

use crate::{
    array::Array,
    datatypes::{DataType, Field},
};

use self::basic::BooleanArrayIterator;
use self::nested::ArrayIterator;
use super::ArrayIter;
use super::{nested_utils::NestedArrayIter, DataPages};

/// Converts [`DataPages`] to an [`Iterator`] of [`Array`]
pub fn iter_to_arrays<'a, I: 'a>(iter: I, data_type: DataType, chunk_size: usize) -> ArrayIter<'a>
where
    I: DataPages,
{
    Box::new(
        BooleanArrayIterator::new(iter, data_type, chunk_size)
            .map(|x| x.map(|x| Arc::new(x) as Arc<dyn Array>)),
    )
}

/// Converts [`DataPages`] to an [`Iterator`] of [`Array`]
pub fn iter_to_arrays_nested<'a, I: 'a>(
    iter: I,
    field: Field,
    chunk_size: usize,
) -> NestedArrayIter<'a>
where
    I: DataPages,
{
    Box::new(ArrayIterator::new(iter, field, chunk_size).map(|x| {
        x.map(|(nested, array)| {
            let values = Arc::new(array) as Arc<dyn Array>;
            (nested, values)
        })
    }))
}
