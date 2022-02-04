use std::{collections::VecDeque, sync::Arc};

use parquet2::{
    page::{DictPage, PrimitivePageDict},
    types::NativeType as ParquetNativeType,
};

use crate::{
    array::{Array, DictionaryArray, DictionaryKey, PrimitiveArray},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::Result,
    io::parquet::read::utils::MaybeNext,
    types::NativeType,
};

use super::super::dictionary::*;
use super::super::DataPages;

#[inline]
fn read_dict<P, T, F>(data_type: DataType, op: F, dict: &dyn DictPage) -> Arc<dyn Array>
where
    T: NativeType,
    P: ParquetNativeType,
    F: Copy + Fn(P) -> T,
{
    let dict = dict
        .as_any()
        .downcast_ref::<PrimitivePageDict<P>>()
        .unwrap();
    let values = dict.values().iter().map(|x| (op)(*x)).collect::<Vec<_>>();

    Arc::new(PrimitiveArray::from_data(data_type, values.into(), None))
}

/// An iterator adapter over [`DataPages`] assumed to be encoded as boolean arrays
#[derive(Debug)]
pub struct DictIter<K, T, I, P, F>
where
    I: DataPages,
    T: NativeType,
    K: DictionaryKey,
    P: ParquetNativeType,
    F: Fn(P) -> T,
{
    iter: I,
    data_type: DataType,
    values: Dict,
    items: VecDeque<(Vec<K>, MutableBitmap)>,
    chunk_size: usize,
    op: F,
    phantom: std::marker::PhantomData<P>,
}

impl<K, T, I, P, F> DictIter<K, T, I, P, F>
where
    K: DictionaryKey,
    I: DataPages,
    T: NativeType,

    P: ParquetNativeType,
    F: Copy + Fn(P) -> T,
{
    pub fn new(iter: I, data_type: DataType, chunk_size: usize, op: F) -> Self {
        let data_type = match data_type {
            DataType::Dictionary(_, values, _) => *values,
            _ => data_type,
        };
        Self {
            iter,
            data_type,
            values: Dict::Empty,
            items: VecDeque::new(),
            chunk_size,
            op,
            phantom: Default::default(),
        }
    }
}

impl<K, T, I, P, F> Iterator for DictIter<K, T, I, P, F>
where
    I: DataPages,
    T: NativeType,
    K: DictionaryKey,
    P: ParquetNativeType,
    F: Copy + Fn(P) -> T,
{
    type Item = Result<DictionaryArray<K>>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_state = next_dict(
            &mut self.iter,
            &mut self.items,
            &mut self.values,
            self.chunk_size,
            |dict| read_dict::<P, T, _>(self.data_type.clone(), self.op, dict),
        );
        match maybe_state {
            MaybeNext::Some(Ok(dict)) => Some(Ok(dict)),
            MaybeNext::Some(Err(e)) => Some(Err(e)),
            MaybeNext::None => None,
            MaybeNext::More => self.next(),
        }
    }
}
