use crate::{
    array::{Array, BooleanArray},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::{ArrowError, Result},
};

use parquet2::{metadata::ColumnChunkMetaData, page::DataPage, FallibleStreamingIterator};

mod basic;
mod nested;

pub use basic::stream_to_array;

use super::nested_utils::Nested;

pub fn iter_to_array<I, E>(
    mut iter: I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
    nested: &mut Vec<Box<dyn Nested>>,
) -> Result<Box<dyn Array>>
where
    ArrowError: From<E>,
    I: FallibleStreamingIterator<Item = DataPage, Error = E>,
{
    let capacity = metadata.num_values() as usize;
    let mut values = MutableBitmap::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);

    let is_nullable = nested.pop().unwrap().is_nullable();

    if nested.is_empty() {
        while let Some(page) = iter.next()? {
            basic::extend_from_page(page, metadata.descriptor(), &mut values, &mut validity)?
        }
    } else {
        while let Some(page) = iter.next()? {
            nested::extend_from_page(
                page,
                metadata.descriptor(),
                is_nullable,
                nested,
                &mut values,
                &mut validity,
            )?
        }
    }

    Ok(Box::new(BooleanArray::from_data(
        data_type,
        values.into(),
        validity.into(),
    )))
}
