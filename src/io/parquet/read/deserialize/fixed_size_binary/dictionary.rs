use std::{collections::VecDeque, sync::Arc};

use parquet2::page::{DictPage, FixedLenByteArrayPageDict};

use crate::{
    array::{Array, DictionaryArray, DictionaryKey, FixedSizeBinaryArray},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::Result,
};

use super::super::dictionary::*;
use super::super::utils::MaybeNext;
use super::super::DataPages;

/// An iterator adapter over [`DataPages`] assumed to be encoded as parquet's dictionary-encoded binary representation
#[derive(Debug)]
pub struct DictIter<K, I>
where
    I: DataPages,
    K: DictionaryKey,
{
    iter: I,
    data_type: DataType,
    values: Dict,
    items: VecDeque<(Vec<K>, MutableBitmap)>,
    chunk_size: Option<usize>,
}

impl<K, I> DictIter<K, I>
where
    K: DictionaryKey,
    I: DataPages,
{
    pub fn new(iter: I, data_type: DataType, chunk_size: Option<usize>) -> Self {
        let data_type = match data_type {
            DataType::Dictionary(_, values, _) => values.as_ref().clone(),
            _ => unreachable!(),
        };
        Self {
            iter,
            data_type,
            values: Dict::Empty,
            items: VecDeque::new(),
            chunk_size,
        }
    }
}

fn read_dict(data_type: DataType, dict: &dyn DictPage) -> Arc<dyn Array> {
    let dict = dict
        .as_any()
        .downcast_ref::<FixedLenByteArrayPageDict>()
        .unwrap();
    let values = dict.values().to_vec();

    Arc::new(FixedSizeBinaryArray::from_data(
        data_type,
        values.into(),
        None,
    ))
}

impl<K, I> Iterator for DictIter<K, I>
where
    I: DataPages,
    K: DictionaryKey,
{
    type Item = Result<DictionaryArray<K>>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_state = next_dict(
            &mut self.iter,
            &mut self.items,
            &mut self.values,
            self.chunk_size,
            |dict| read_dict(self.data_type.clone(), dict),
        );
        match maybe_state {
            MaybeNext::Some(Ok(dict)) => Some(Ok(dict)),
            MaybeNext::Some(Err(e)) => Some(Err(e)),
            MaybeNext::None => None,
            MaybeNext::More => self.next(),
        }
    }
}
