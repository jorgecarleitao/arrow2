use std::{collections::VecDeque, sync::Arc};

use parquet2::page::{BinaryPageDict, DictPage};

use crate::{
    array::{Array, BinaryArray, DictionaryArray, DictionaryKey, Offset, Utf8Array},
    bitmap::MutableBitmap,
    datatypes::{DataType, PhysicalType},
    error::Result,
};

use super::super::dictionary::*;
use super::super::utils::MaybeNext;
use super::super::DataPages;

/// An iterator adapter over [`DataPages`] assumed to be encoded as parquet's dictionary-encoded binary representation
#[derive(Debug)]
pub struct DictIter<K, O, I>
where
    I: DataPages,
    O: Offset,
    K: DictionaryKey,
{
    iter: I,
    data_type: DataType,
    values: Dict,
    items: VecDeque<(Vec<K>, MutableBitmap)>,
    chunk_size: Option<usize>,
    phantom: std::marker::PhantomData<O>,
}

impl<K, O, I> DictIter<K, O, I>
where
    K: DictionaryKey,
    O: Offset,
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
            phantom: std::marker::PhantomData,
        }
    }
}

fn read_dict<O: Offset>(data_type: DataType, dict: &dyn DictPage) -> Arc<dyn Array> {
    let dict = dict.as_any().downcast_ref::<BinaryPageDict>().unwrap();
    let offsets = dict
        .offsets()
        .iter()
        .map(|x| O::from_usize(*x as usize).unwrap())
        .collect::<Vec<_>>();
    let values = dict.values().to_vec();

    match data_type.to_physical_type() {
        PhysicalType::Utf8 | PhysicalType::LargeUtf8 => Arc::new(Utf8Array::<O>::from_data(
            data_type,
            offsets.into(),
            values.into(),
            None,
        )) as _,
        PhysicalType::Binary | PhysicalType::LargeBinary => Arc::new(BinaryArray::<O>::from_data(
            data_type,
            offsets.into(),
            values.into(),
            None,
        )) as _,
        _ => unreachable!(),
    }
}

impl<K, O, I> Iterator for DictIter<K, O, I>
where
    I: DataPages,
    O: Offset,
    K: DictionaryKey,
{
    type Item = Result<DictionaryArray<K>>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_state = next_dict(
            &mut self.iter,
            &mut self.items,
            &mut self.values,
            self.chunk_size,
            |dict| read_dict::<O>(self.data_type.clone(), dict),
        );
        match maybe_state {
            MaybeNext::Some(Ok(dict)) => Some(Ok(dict)),
            MaybeNext::Some(Err(e)) => Some(Err(e)),
            MaybeNext::None => None,
            MaybeNext::More => self.next(),
        }
    }
}
