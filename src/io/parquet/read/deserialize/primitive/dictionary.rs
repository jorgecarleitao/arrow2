use std::collections::VecDeque;

use parquet2::{
    page::{DictPage, PrimitivePageDict},
    types::NativeType as ParquetNativeType,
};

use crate::{
    array::{Array, DictionaryArray, DictionaryKey, PrimitiveArray},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::Result,
    types::NativeType,
};

use super::super::dictionary::nested_next_dict;
use super::super::dictionary::*;
use super::super::nested_utils::{InitNested, NestedArrayIter, NestedState};
use super::super::utils::MaybeNext;
use super::super::DataPages;

fn read_dict<P, T, F>(data_type: DataType, op: F, dict: &dyn DictPage) -> Box<dyn Array>
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

    Box::new(PrimitiveArray::new(data_type, values.into(), None))
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
    values_data_type: DataType,
    values: Dict,
    items: VecDeque<(Vec<K>, MutableBitmap)>,
    chunk_size: Option<usize>,
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
    pub fn new(iter: I, data_type: DataType, chunk_size: Option<usize>, op: F) -> Self {
        let values_data_type = match &data_type {
            DataType::Dictionary(_, values, _) => *(values.clone()),
            _ => unreachable!(),
        };
        Self {
            iter,
            data_type,
            values_data_type,
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
            self.data_type.clone(),
            self.chunk_size,
            |dict| read_dict::<P, T, _>(self.values_data_type.clone(), self.op, dict),
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
pub struct NestedDictIter<K, T, I, P, F>
where
    I: DataPages,
    T: NativeType,
    K: DictionaryKey,
    P: ParquetNativeType,
    F: Fn(P) -> T,
{
    iter: I,
    init: Vec<InitNested>,
    data_type: DataType,
    values: Dict,
    // invariant: items.len() == nested.len()
    items: VecDeque<(Vec<K>, MutableBitmap)>,
    nested: VecDeque<NestedState>,
    chunk_size: Option<usize>,
    op: F,
    phantom: std::marker::PhantomData<P>,
}

impl<K, T, I, P, F> NestedDictIter<K, T, I, P, F>
where
    K: DictionaryKey,
    I: DataPages,
    T: NativeType,

    P: ParquetNativeType,
    F: Copy + Fn(P) -> T,
{
    pub fn new(
        iter: I,
        init: Vec<InitNested>,
        data_type: DataType,
        chunk_size: Option<usize>,
        op: F,
    ) -> Self {
        let data_type = match data_type {
            DataType::Dictionary(_, values, _) => *values,
            _ => data_type,
        };
        Self {
            iter,
            init,
            data_type,
            values: Dict::Empty,
            items: VecDeque::new(),
            nested: VecDeque::new(),
            chunk_size,
            op,
            phantom: Default::default(),
        }
    }
}

impl<K, T, I, P, F> Iterator for NestedDictIter<K, T, I, P, F>
where
    I: DataPages,
    T: NativeType,
    K: DictionaryKey,
    P: ParquetNativeType,
    F: Copy + Fn(P) -> T,
{
    type Item = Result<(NestedState, DictionaryArray<K>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_state = nested_next_dict(
            &mut self.iter,
            &mut self.items,
            &mut self.nested,
            &self.init,
            &mut self.values,
            self.data_type.clone(),
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

/// Converts [`DataPages`] to an [`Iterator`] of [`Array`]
pub fn iter_to_arrays_nested<'a, K, I, T, P, F>(
    iter: I,
    init: Vec<InitNested>,
    data_type: DataType,
    chunk_size: Option<usize>,
    op: F,
) -> NestedArrayIter<'a>
where
    I: 'a + DataPages,
    K: DictionaryKey,
    T: crate::types::NativeType,
    P: parquet2::types::NativeType,
    F: 'a + Copy + Send + Sync + Fn(P) -> T,
{
    Box::new(
        NestedDictIter::<K, _, _, _, _>::new(iter, init, data_type, chunk_size, op).map(|result| {
            let (mut nested, array) = result?;
            let _ = nested.nested.pop().unwrap(); // the primitive
            Ok((nested, array.boxed()))
        }),
    )
}
