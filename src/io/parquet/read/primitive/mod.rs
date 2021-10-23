mod basic;
mod dictionary;
mod nested;
mod utils;

use std::sync::Arc;

use futures::{pin_mut, Stream, StreamExt};
use parquet2::{page::DataPage, types::NativeType, FallibleStreamingIterator};

use super::nested_utils::*;
use super::{ColumnChunkMetaData, ColumnDescriptor};
use crate::{
    array::{Array, PrimitiveArray},
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
    datatypes::DataType,
    error::{ArrowError, Result},
    types::NativeType as ArrowNativeType,
};

pub use dictionary::iter_to_array as iter_to_dict_array;

pub fn iter_to_array<T, A, I, E, F>(
    mut iter: I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
    op: F,
) -> Result<Box<dyn Array>>
where
    ArrowError: From<E>,
    T: NativeType,
    A: ArrowNativeType,
    F: Copy + Fn(T) -> A,
    I: FallibleStreamingIterator<Item = DataPage, Error = E>,
{
    let capacity = metadata.num_values() as usize;
    let mut values = MutableBuffer::<A>::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);
    while let Some(page) = iter.next()? {
        basic::extend_from_page(page, metadata.descriptor(), &mut values, &mut validity, op)?
    }

    let data_type = match data_type {
        DataType::Dictionary(_, values) => values.as_ref().clone(),
        _ => data_type,
    };

    Ok(Box::new(PrimitiveArray::from_data(
        data_type,
        values.into(),
        validity.into(),
    )))
}

pub async fn stream_to_array<T, A, I, E, F>(
    pages: I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
    op: F,
) -> Result<Box<dyn Array>>
where
    ArrowError: From<E>,
    T: NativeType,
    E: Clone,
    A: ArrowNativeType,
    F: Copy + Fn(T) -> A,
    I: Stream<Item = std::result::Result<DataPage, E>>,
{
    let capacity = metadata.num_values() as usize;
    let mut values = MutableBuffer::<A>::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);

    pin_mut!(pages); // needed for iteration

    while let Some(page) = pages.next().await {
        basic::extend_from_page(
            page.as_ref().map_err(|x| x.clone())?,
            metadata.descriptor(),
            &mut values,
            &mut validity,
            op,
        )?
    }

    let data_type = match data_type {
        DataType::Dictionary(_, values) => values.as_ref().clone(),
        _ => data_type,
    };

    Ok(Box::new(PrimitiveArray::from_data(
        data_type,
        values.into(),
        validity.into(),
    )))
}

pub fn iter_to_array_nested<T, A, I, E, F>(
    mut iter: I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
    op: F,
) -> Result<(Arc<dyn Array>, Vec<Box<dyn Nested>>)>
where
    ArrowError: From<E>,
    T: NativeType,
    E: Clone,
    A: ArrowNativeType,
    F: Copy + Fn(T) -> A,
    I: FallibleStreamingIterator<Item = DataPage, Error = E>,
{
    let capacity = metadata.num_values() as usize;
    let mut values = MutableBuffer::<A>::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);

    let (mut nested, is_nullable) = init_nested(metadata.descriptor().base_type(), capacity);

    while let Some(page) = iter.next()? {
        nested::extend_from_page(
            page,
            metadata.descriptor(),
            is_nullable,
            &mut nested,
            &mut values,
            &mut validity,
            op,
        )?
    }

    let values = Arc::new(PrimitiveArray::<A>::from_data(
        data_type,
        values.into(),
        validity.into(),
    ));
    Ok((values, nested))
}
