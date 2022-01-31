use std::{collections::VecDeque, sync::Arc};

use parquet2::page::FixedLenByteArrayPageDict;

use crate::{
    array::{Array, DictionaryArray, DictionaryKey, FixedSizeBinaryArray, PrimitiveArray},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::{ArrowError, Result},
};

use super::super::dictionary::*;
use super::super::utils;
use super::super::utils::Decoder;
use super::super::ArrayIter;
use super::super::DataPages;

/// An iterator adapter over [`DataPages`] assumed to be encoded as parquet's dictionary-encoded binary representation
#[derive(Debug)]
pub struct ArrayIterator<K, I>
where
    I: DataPages,
    K: DictionaryKey,
{
    iter: I,
    data_type: DataType,
    values: Dict,
    items: VecDeque<(Vec<K>, MutableBitmap)>,
    chunk_size: usize,
}

impl<K, I> ArrayIterator<K, I>
where
    K: DictionaryKey,
    I: DataPages,
{
    fn new(iter: I, data_type: DataType, chunk_size: usize) -> Self {
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

impl<K, I> Iterator for ArrayIterator<K, I>
where
    I: DataPages,
    K: DictionaryKey,
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
                        .downcast_ref::<FixedLenByteArrayPageDict>()
                        .unwrap();
                    self.values = match &mut self.values {
                        Dict::Empty => {
                            let values = dict.values().to_vec();

                            let array = Arc::new(FixedSizeBinaryArray::from_data(
                                self.data_type.clone(),
                                values.into(),
                                None,
                            )) as _;
                            Dict::Complete(array)
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
                    let maybe_page = PrimitiveDecoder::default().build_state(page);
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
                        let keys = PrimitiveArray::from_data(
                            K::PRIMITIVE.into(),
                            values.into(),
                            validity.into(),
                        );

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

                let keys = finish_key(values, validity);

                let values = self.values.unwrap();
                Some(Ok(DictionaryArray::from_data(keys, values)))
            }
        }
    }
}

/// Converts [`DataPages`] to an [`Iterator`] of [`Array`]
pub fn iter_to_arrays<'a, K, I>(iter: I, data_type: DataType, chunk_size: usize) -> ArrayIter<'a>
where
    I: 'a + DataPages,
    K: DictionaryKey,
{
    Box::new(
        ArrayIterator::<K, I>::new(iter, data_type, chunk_size)
            .map(|x| x.map(|x| Arc::new(x) as Arc<dyn Array>)),
    )
}
