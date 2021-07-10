mod basic;
mod nested;
mod utils;

use std::sync::Arc;

use parquet2::{
    read::{Page, StreamingIterator},
    types::NativeType,
};

use super::nested_utils::*;
use super::{ColumnChunkMetaData, ColumnDescriptor};
use crate::{
    array::{Array, ListArray, PrimitiveArray},
    bitmap::MutableBitmap,
    buffer::{Buffer, MutableBuffer},
    datatypes::DataType,
    error::{ArrowError, Result},
    types::NativeType as ArrowNativeType,
};

pub fn iter_to_array<T, A, I, E, F>(
    mut iter: I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
    op: F,
) -> Result<PrimitiveArray<A>>
where
    ArrowError: From<E>,
    T: NativeType,
    E: Clone,
    A: ArrowNativeType,
    F: Copy + Fn(T) -> A,
    I: StreamingIterator<Item = std::result::Result<Page, E>>,
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

    Ok(PrimitiveArray::from_data(
        data_type,
        values.into(),
        validity.into(),
    ))
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
    I: StreamingIterator<Item = std::result::Result<Page, E>>,
{
    let capacity = metadata.num_values() as usize;
    let mut values = MutableBuffer::<A>::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);

    let mut nullable = Vec::new();
    is_nullable(metadata.descriptor().base_type(), &mut nullable);
    // the primitive's nullability is the last on the list
    let is_nullable = nullable.pop().unwrap();

    let mut nested = nullable
        .iter()
        .map(|is_nullable| {
            if *is_nullable {
                Box::new(NestedOptional::with_capacity(capacity)) as Box<dyn Nested>
            } else {
                Box::new(NestedValid::with_capacity(capacity)) as Box<dyn Nested>
            }
        })
        .collect::<Vec<_>>();

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

    Ok(match data_type {
        DataType::List(ref inner) => {
            let values = Arc::new(PrimitiveArray::<A>::from_data(
                inner.data_type().clone(),
                values.into(),
                validity.into(),
            ));

            let (offsets, validity) = nested[0].inner();

            let offsets = Buffer::<i32>::from_trusted_len_iter(offsets.iter().map(|x| *x as i32));
            Box::new(ListArray::<i32>::from_data(
                data_type, offsets, values, validity,
            ))
        }
        DataType::LargeList(ref inner) => {
            let values = Arc::new(PrimitiveArray::<A>::from_data(
                inner.data_type().clone(),
                values.into(),
                validity.into(),
            ));

            let (offsets, validity) = nested[0].inner();

            Box::new(ListArray::<i64>::from_data(
                data_type, offsets, values, validity,
            ))
        }
        _ => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Read nested datatype {:?}",
                data_type
            )))
        }
    })
}
