use std::collections::VecDeque;

use parquet2::page::{BinaryPageDict, DictPage};

use crate::{
    array::{Array, BinaryArray, DictionaryArray, DictionaryKey, Offset, Utf8Array},
    bitmap::MutableBitmap,
    datatypes::{DataType, PhysicalType},
    error::Result,
    io::parquet::read::deserialize::nested_utils::{InitNested, NestedArrayIter, NestedState},
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

fn read_dict<O: Offset>(data_type: DataType, dict: &dyn DictPage) -> Box<dyn Array> {
    let data_type = match data_type {
        DataType::Dictionary(_, values, _) => *values,
        _ => data_type,
    };

    let dict = dict.as_any().downcast_ref::<BinaryPageDict>().unwrap();
    let offsets = dict
        .offsets()
        .iter()
        .map(|x| O::from_usize(*x as usize).unwrap())
        .collect::<Vec<_>>();
    let values = dict.values().to_vec();

    match data_type.to_physical_type() {
        PhysicalType::Utf8 | PhysicalType::LargeUtf8 => Box::new(Utf8Array::<O>::from_data(
            data_type,
            offsets.into(),
            values.into(),
            None,
        )) as _,
        PhysicalType::Binary | PhysicalType::LargeBinary => Box::new(BinaryArray::<O>::from_data(
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
            self.data_type.clone(),
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

#[derive(Debug)]
pub struct NestedDictIter<K, O, I>
where
    I: DataPages,
    O: Offset,
    K: DictionaryKey,
{
    iter: I,
    init: Vec<InitNested>,
    data_type: DataType,
    values: Dict,
    items: VecDeque<(NestedState, (Vec<K>, MutableBitmap))>,
    chunk_size: Option<usize>,
    phantom: std::marker::PhantomData<O>,
}

impl<K, O, I> NestedDictIter<K, O, I>
where
    I: DataPages,
    O: Offset,
    K: DictionaryKey,
{
    pub fn new(
        iter: I,
        init: Vec<InitNested>,
        data_type: DataType,
        chunk_size: Option<usize>,
    ) -> Self {
        Self {
            iter,
            init,
            data_type,
            values: Dict::Empty,
            items: VecDeque::new(),
            chunk_size,
            phantom: Default::default(),
        }
    }
}

impl<K, O, I> Iterator for NestedDictIter<K, O, I>
where
    I: DataPages,
    O: Offset,
    K: DictionaryKey,
{
    type Item = Result<(NestedState, DictionaryArray<K>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_state = nested_next_dict(
            &mut self.iter,
            &mut self.items,
            &self.init,
            &mut self.values,
            self.data_type.clone(),
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

/// Converts [`DataPages`] to an [`Iterator`] of [`Array`]
pub fn iter_to_arrays_nested<'a, K, O, I>(
    iter: I,
    init: Vec<InitNested>,
    data_type: DataType,
    chunk_size: Option<usize>,
) -> NestedArrayIter<'a>
where
    I: 'a + DataPages,
    O: Offset,
    K: DictionaryKey,
{
    Box::new(
        NestedDictIter::<K, O, I>::new(iter, init, data_type, chunk_size).map(|result| {
            let (mut nested, array) = result?;
            let _ = nested.nested.pop().unwrap(); // the primitive
            Ok((nested, array.boxed()))
        }),
    )
}
