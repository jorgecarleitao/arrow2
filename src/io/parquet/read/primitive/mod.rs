mod basic;
mod dictionary;
mod nested;
mod utils;

use std::sync::Arc;

use super::ColumnDescriptor;
use super::{nested_utils::*, DataPages};
use crate::{array::Array, datatypes::DataType, error::Result};

use basic::PrimitiveArrayIterator;

pub use dictionary::iter_to_arrays as iter_to_dict_arrays;

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

pub use utils::read_item;
