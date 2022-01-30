mod basic;
mod dictionary;
mod nested;
mod utils;

use std::sync::Arc;

use futures::{pin_mut, Stream, StreamExt};
use parquet2::{page::DataPage, types::NativeType, FallibleStreamingIterator};

use super::{nested_utils::*, DataPages};
use super::{ColumnChunkMetaData, ColumnDescriptor};
use crate::{
    array::{Array, PrimitiveArray},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::{ArrowError, Result},
    types::NativeType as ArrowNativeType,
};

use basic::PrimitiveArrayIterator;

pub use dictionary::iter_to_array as iter_to_dict_array;

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
    let mut values = Vec::<A>::with_capacity(capacity);
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
        DataType::Dictionary(_, values, _) => values.as_ref().clone(),
        _ => data_type,
    };

    Ok(Box::new(PrimitiveArray::from_data(
        data_type,
        values.into(),
        validity.into(),
    )))
}

pub fn iter_to_array<T, A, I, E, F>(
    mut iter: I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
    nested: &mut Vec<Box<dyn Nested>>,
    op: F,
) -> Result<Box<dyn Array>>
where
    ArrowError: From<E>,
    T: NativeType,
    E: Clone,
    A: ArrowNativeType,
    F: Copy + Fn(T) -> A,
    I: FallibleStreamingIterator<Item = DataPage, Error = E>,
{
    let is_nullable = nested.pop().unwrap().is_nullable();
    let capacity = metadata.num_values() as usize;
    let mut values = Vec::<A>::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity * usize::from(is_nullable));

    if nested.is_empty() {
        while let Some(page) = iter.next()? {
            basic::extend_from_page(page, metadata.descriptor(), &mut values, &mut validity, op)?
        }
        debug_assert_eq!(values.len(), capacity);
        debug_assert_eq!(validity.len(), capacity * usize::from(is_nullable));
    } else {
        while let Some(page) = iter.next()? {
            nested::extend_from_page(
                page,
                metadata.descriptor(),
                is_nullable,
                nested,
                &mut values,
                &mut validity,
                op,
            )?
        }
    }

    let data_type = match data_type {
        DataType::Dictionary(_, values, _) => values.as_ref().clone(),
        _ => data_type,
    };

    Ok(Box::new(PrimitiveArray::from_data(
        data_type,
        values.into(),
        validity.into(),
    )))
}

fn deserialize_page<T: NativeType, A: ArrowNativeType, F: Fn(T) -> A>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    data_type: DataType,
    op: F,
) -> Result<Box<dyn Array>> {
    let capacity = page.num_values() as usize;
    let mut values = Vec::<A>::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);

    basic::extend_from_page(page, descriptor, &mut values, &mut validity, op)?;

    let data_type = match data_type {
        DataType::Dictionary(_, values, _) => values.as_ref().clone(),
        _ => data_type,
    };

    Ok(Box::new(PrimitiveArray::from_data(
        data_type,
        values.into(),
        validity.into(),
    )))
}

/// Converts [`DataPages`] to an [`Iterator`] of [`Array`]
pub fn iter_to_arrays<'a, I, T, P, G, F>(
    iter: I,
    is_optional: bool,
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
        PrimitiveArrayIterator::<T, I, P, G, F>::new(
            iter,
            data_type,
            chunk_size,
            is_optional,
            op1,
            op2,
        )
        .map(|x| x.map(|x| Arc::new(x) as Arc<dyn Array>)),
    )
}

pub use utils::read_item;
