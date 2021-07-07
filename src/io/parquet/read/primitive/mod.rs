mod basic;
mod nested;
mod utils;

use std::sync::Arc;

use parquet2::{
    read::{Page, StreamingIterator},
    types::NativeType,
};

use super::{ColumnChunkMetaData, ColumnDescriptor};
use crate::{
    array::{Array, ListArray, PrimitiveArray},
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
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

#[derive(Debug, Default)]
pub struct Nested {
    pub validity: MutableBitmap,
    pub offsets: MutableBuffer<i64>,
}

impl Nested {
    pub fn new(capacity: usize) -> Self {
        let mut offsets = MutableBuffer::<i64>::with_capacity(capacity + 1);
        offsets.push(0);
        let validity = MutableBitmap::with_capacity(capacity);
        Self { validity, offsets }
    }
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

    // -2: 1 for the null buffer of the primitive type; 1 for the emptyness of the list
    let mut nested = (0..metadata.descriptor().max_def_level() - 2)
        .map(|_| Nested::new(capacity))
        .collect::<Vec<_>>();

    while let Some(page) = iter.next() {
        nested::extend_from_page(
            page.as_ref().map_err(|x| x.clone())?,
            metadata.descriptor(),
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

            let nested = std::mem::take(&mut nested[0]);
            let Nested { offsets, validity } = nested;

            let offsets =
                MutableBuffer::<i32>::from_trusted_len_iter(offsets.iter().map(|x| *x as i32));
            Box::new(ListArray::<i32>::from_data(
                data_type,
                offsets.into(),
                values,
                validity.into(),
            ))
        }
        DataType::LargeList(ref inner) => {
            let values = Arc::new(PrimitiveArray::<A>::from_data(
                inner.data_type().clone(),
                values.into(),
                validity.into(),
            ));

            let nested = std::mem::take(&mut nested[0]);
            let Nested { offsets, validity } = nested;

            Box::new(ListArray::<i64>::from_data(
                data_type,
                offsets.into(),
                values,
                validity.into(),
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
