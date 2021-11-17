use futures::{pin_mut, Stream, StreamExt};
use parquet2::{metadata::ColumnChunkMetaData, page::DataPage, FallibleStreamingIterator};

use crate::{
    array::{Array, Offset},
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
    datatypes::DataType,
    error::{ArrowError, Result},
    io::parquet::read::binary::utils::finish_array,
};

mod basic;
mod dictionary;
mod nested;
mod utils;

pub use dictionary::iter_to_array as iter_to_dict_array;

use super::nested_utils::Nested;

pub fn iter_to_array<O, I, E>(
    mut iter: I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
    nested: &mut Vec<Box<dyn Nested>>,
) -> Result<Box<dyn Array>>
where
    O: Offset,
    ArrowError: From<E>,
    I: FallibleStreamingIterator<Item = DataPage, Error = E>,
{
    let capacity = metadata.num_values() as usize;
    let mut values = MutableBuffer::<u8>::with_capacity(0);
    let mut offsets = MutableBuffer::<O>::with_capacity(1 + capacity);
    offsets.push(O::default());
    let mut validity = MutableBitmap::with_capacity(capacity);

    let is_nullable = nested.pop().unwrap().is_nullable();

    if nested.is_empty() {
        while let Some(page) = iter.next()? {
            basic::extend_from_page(
                page,
                metadata.descriptor(),
                &mut offsets,
                &mut values,
                &mut validity,
            )?
        }
    } else {
        while let Some(page) = iter.next()? {
            nested::extend_from_page(
                page,
                metadata.descriptor(),
                is_nullable,
                nested,
                &mut offsets,
                &mut values,
                &mut validity,
            )?
        }
    }
    Ok(utils::finish_array(data_type, offsets, values, validity))
}

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
    let mut values = MutableBuffer::<u8>::with_capacity(0);
    let mut offsets = MutableBuffer::<O>::with_capacity(1 + capacity);
    offsets.push(O::default());
    let mut validity = MutableBitmap::with_capacity(capacity);

    pin_mut!(pages); // needed for iteration

    while let Some(page) = pages.next().await {
        basic::extend_from_page(
            page.as_ref().map_err(|x| x.clone())?,
            metadata.descriptor(),
            &mut offsets,
            &mut values,
            &mut validity,
        )?
    }

    Ok(finish_array(data_type.clone(), offsets, values, validity))
}
