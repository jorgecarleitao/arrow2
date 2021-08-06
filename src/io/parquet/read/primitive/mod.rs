mod basic;
mod dictionary;
mod nested;
mod utils;

use std::sync::Arc;

use parquet2::{page::DataPage, read::StreamingIterator, types::NativeType};

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
    E: Clone,
    A: ArrowNativeType,
    F: Copy + Fn(T) -> A,
    I: StreamingIterator<Item = std::result::Result<DataPage, E>>,
{
    let capacity = metadata.num_values() as usize;
    let mut values = MutableBuffer::<A>::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);
    while let Some(page) = iter.next() {
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
) -> Result<Box<dyn Array>>
where
    ArrowError: From<E>,
    T: NativeType,
    E: Clone,
    A: ArrowNativeType,
    F: Copy + Fn(T) -> A,
    I: StreamingIterator<Item = std::result::Result<DataPage, E>>,
{
    let capacity = metadata.num_values() as usize;
    let mut values = MutableBuffer::<A>::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);

    let (mut nested, is_nullable) = init_nested(metadata.descriptor().base_type(), capacity);

    while let Some(page) = iter.next() {
        nested::extend_from_page(
            page.as_ref().map_err(|x| x.clone())?,
            metadata.descriptor(),
            is_nullable,
            &mut nested,
            &mut values,
            &mut validity,
            op,
        )?
    }

    let values = match data_type {
        DataType::List(ref inner) => Arc::new(PrimitiveArray::<A>::from_data(
            inner.data_type().clone(),
            values.into(),
            validity.into(),
        )),
        DataType::LargeList(ref inner) => Arc::new(PrimitiveArray::<A>::from_data(
            inner.data_type().clone(),
            values.into(),
            validity.into(),
        )),
        _ => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Read nested datatype {:?}",
                data_type
            )))
        }
    };

    create_list(data_type, &mut nested, values)
}
