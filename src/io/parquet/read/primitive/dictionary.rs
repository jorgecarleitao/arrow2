use std::{collections::VecDeque, sync::Arc};

use parquet2::{page::PrimitivePageDict, types::NativeType as ParquetNativeType};

use crate::{
    array::{Array, DictionaryArray, DictionaryKey, PrimitiveArray},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::{ArrowError, Result},
    types::NativeType,
};

use super::super::dictionary::*;
use super::super::utils;
use super::super::utils::Decoder;
use super::super::ArrayIter;
use super::super::DataPages;

/// An iterator adapter over [`DataPages`] assumed to be encoded as boolean arrays
#[derive(Debug)]
pub struct ArrayIterator<K, T, I, P, F>
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

impl<K, T, I, P, F> ArrayIterator<K, T, I, P, F>
where
    K: DictionaryKey,
    I: DataPages,
    T: NativeType,

    P: ParquetNativeType,
    F: Copy + Fn(P) -> T,
{
    fn new(iter: I, data_type: DataType, chunk_size: usize, op: F) -> Self {
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

impl<K, T, I, P, F> Iterator for ArrayIterator<K, T, I, P, F>
where
    I: DataPages,
    T: NativeType,
    K: DictionaryKey,
    P: ParquetNativeType,
    F: Copy + Fn(P) -> T,
{
    type Item = Result<DictionaryArray<K>>;

    fn next(&mut self) -> Option<Self::Item> {
        // back[a1, a2, a3, ...]front
        if self.items.len() > 1 {
            return self.items.pop_back().map(|(values, validity)| {
                let keys = finish_key(values, validity);
                let values = self.values.unwrap();
                Ok(DictionaryArray::from_data(keys, values))
            });
        }
        match (self.items.pop_back(), self.iter.next()) {
            (_, Err(e)) => Some(Err(e.into())),
            (None, Ok(None)) => None,
            (state, Ok(Some(page))) => {
                // consume the dictionary page
                if let Some(dict) = page.dictionary_page() {
                    let dict = dict
                        .as_any()
                        .downcast_ref::<PrimitivePageDict<P>>()
                        .unwrap();
                    self.values = match &mut self.values {
                        Dict::Empty => {
                            let values = dict
                                .values()
                                .iter()
                                .map(|x| (self.op)(*x))
                                .collect::<Vec<_>>();

                            Dict::Complete(Arc::new(PrimitiveArray::from_data(
                                self.data_type.clone(),
                                values.into(),
                                None,
                            )) as _)
                        }
                        _ => unreachable!(),
                    };
                } else {
                    return Some(Err(ArrowError::nyi(
                        "dictionary arrays from non-dict-encoded pages",
                    )));
                }

                let maybe_array = {
                    // there is a new page => consume the page from the start
                    let decoder = PrimitiveDecoder::default();
                    let maybe_page = decoder.build_state(page);
                    let page = match maybe_page {
                        Ok(page) => page,
                        Err(e) => return Some(Err(e)),
                    };

                    utils::extend_from_new_page::<PrimitiveDecoder<K>, _, _>(
                        page,
                        state,
                        self.chunk_size,
                        &mut self.items,
                        &PrimitiveDecoder::default(),
                    )
                };
                match maybe_array {
                    Ok(Some((values, validity))) => {
                        let keys = finish_key(values, validity);

                        let values = self.values.unwrap();
                        Some(Ok(DictionaryArray::from_data(keys, values)))
                    }
                    Ok(None) => self.next(),
                    Err(e) => Some(Err(e)),
                }
            }
            (Some((values, validity)), Ok(None)) => {
                // we have a populated item and no more pages
                // the only case where an item's length may be smaller than chunk_size
                debug_assert!(values.len() <= self.chunk_size);

                let keys =
                    PrimitiveArray::from_data(K::PRIMITIVE.into(), values.into(), validity.into());

                let values = self.values.unwrap();
                Some(Ok(DictionaryArray::from_data(keys, values)))
            }
        }
    }
}

/// Converts [`DataPages`] to an [`Iterator`] of [`Array`]
pub fn iter_to_arrays<'a, K, I, T, P, F>(
    iter: I,
    data_type: DataType,
    chunk_size: usize,
    op: F,
) -> ArrayIter<'a>
where
    I: 'a + DataPages,
    K: DictionaryKey,
    T: NativeType,
    P: ParquetNativeType,
    F: 'a + Copy + Send + Sync + Fn(P) -> T,
{
    Box::new(
        ArrayIterator::<K, T, I, P, F>::new(iter, data_type, chunk_size, op)
            .map(|x| x.map(|x| Arc::new(x) as Arc<dyn Array>)),
    )
}
