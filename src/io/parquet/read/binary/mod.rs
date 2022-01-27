use std::sync::Arc;

use futures::{pin_mut, Stream, StreamExt};
use parquet2::{metadata::ColumnChunkMetaData, page::DataPage, FallibleStreamingIterator};

use crate::{
    array::{Array, Offset},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::{ArrowError, Result},
    io::parquet::read::binary::utils::finish_array,
};

mod basic;
mod dictionary;
mod nested;
mod utils;

pub use dictionary::iter_to_arrays as iter_to_dict_arrays;

use self::{basic::TraitBinaryArray, utils::Binary};

use super::{nested_utils::Nested, DataPages};
use basic::BinaryArrayIterator;

pub async fn stream_to_array<O, I, E>(
    pages: I,
    metadata: &ColumnChunkMetaData,
    data_type: &DataType,
) -> Result<Box<dyn Array>>
where
    ArrowError: From<E>,
    O: Offset,
    E: Clone,
    I: Stream<Item = std::result::Result<DataPage, E>>,
{
    let capacity = metadata.num_values() as usize;
    let mut values = Binary::<O>::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);

    pin_mut!(pages); // needed for iteration

    while let Some(page) = pages.next().await {
        basic::extend_from_page(
            page.as_ref().map_err(|x| x.clone())?,
            metadata.descriptor(),
            &mut values,
            &mut validity,
        )?
    }

    Ok(finish_array(data_type.clone(), values, validity))
}

/// Converts [`DataPages`] to an [`Iterator`] of [`Array`]
pub fn iter_to_arrays<'a, O, A, I>(
    iter: I,
    is_optional: bool,
    data_type: DataType,
    chunk_size: usize,
) -> Box<dyn Iterator<Item = Result<Arc<dyn Array>>> + 'a>
where
    I: 'a + DataPages,
    A: TraitBinaryArray<O>,
    O: Offset,
{
    Box::new(
        BinaryArrayIterator::<O, A, I>::new(iter, data_type, chunk_size, is_optional)
            .map(|x| x.map(|x| Arc::new(x) as Arc<dyn Array>)),
    )
}
