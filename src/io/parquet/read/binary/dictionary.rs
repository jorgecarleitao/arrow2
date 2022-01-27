use std::{collections::VecDeque, sync::Arc};

use parquet2::page::BinaryPageDict;

use crate::{
    array::{Array, BinaryArray, DictionaryArray, DictionaryKey, Offset, Utf8Array},
    bitmap::MutableBitmap,
    datatypes::{DataType, PhysicalType},
    error::{ArrowError, Result},
};

use super::super::dictionary::*;
use super::super::utils;
use super::super::utils::Decoder;
use super::super::DataPages;

/// An iterator adapter over [`DataPages`] assumed to be encoded as parquet's dictionary-encoded binary representation
#[derive(Debug)]
pub struct ArrayIterator<K, O, I>
where
    I: DataPages,
    O: Offset,
    K: DictionaryKey,
{
    iter: I,
    data_type: DataType,
    values: Dict,
    items: VecDeque<(Vec<K>, MutableBitmap)>,
    chunk_size: usize,
    is_optional: bool,
    phantom: std::marker::PhantomData<O>,
}

impl<K, O, I> ArrayIterator<K, O, I>
where
    K: DictionaryKey,
    O: Offset,
    I: DataPages,
{
    fn new(iter: I, data_type: DataType, chunk_size: usize, is_optional: bool) -> Self {
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
            is_optional,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<K, O, I> Iterator for ArrayIterator<K, O, I>
where
    I: DataPages,
    O: Offset,
    K: DictionaryKey,
{
    type Item = Result<DictionaryArray<K>>;

    fn next(&mut self) -> Option<Self::Item> {
        // back[a1, a2, a3, ...]front
        if self.items.len() > 1 {
            return self.items.pop_back().map(|(values, validity)| {
                let keys = PrimitiveDecoder::<K>::finish(self.data_type.clone(), values, validity);
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
                    let dict = dict.as_any().downcast_ref::<BinaryPageDict>().unwrap();
                    self.values = match &mut self.values {
                        Dict::Empty => {
                            let offsets = dict
                                .offsets()
                                .iter()
                                .map(|x| O::from_usize(*x as usize).unwrap())
                                .collect::<Vec<_>>();
                            let values = dict.values().to_vec();

                            let array = match self.data_type.to_physical_type() {
                                PhysicalType::Utf8 | PhysicalType::LargeUtf8 => {
                                    Arc::new(Utf8Array::<O>::from_data(
                                        self.data_type.clone(),
                                        offsets.into(),
                                        values.into(),
                                        None,
                                    )) as _
                                }
                                PhysicalType::Binary | PhysicalType::LargeBinary => {
                                    Arc::new(BinaryArray::<O>::from_data(
                                        self.data_type.clone(),
                                        offsets.into(),
                                        values.into(),
                                        None,
                                    )) as _
                                }
                                _ => unreachable!(),
                            };

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
                    let maybe_page = build_state(page, self.is_optional);
                    let page = match maybe_page {
                        Ok(page) => page,
                        Err(e) => return Some(Err(e)),
                    };

                    utils::extend_from_new_page::<PrimitiveDecoder<K>, _, _>(
                        page,
                        state,
                        &self.data_type,
                        self.chunk_size,
                        &mut self.items,
                        &PrimitiveDecoder::default(),
                    )
                };
                match maybe_array {
                    Ok(Some(keys)) => {
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

                let keys = PrimitiveDecoder::<K>::finish(self.data_type.clone(), values, validity);
                let values = self.values.unwrap();
                Some(Ok(DictionaryArray::from_data(keys, values)))
            }
        }
    }
}

/// Converts [`DataPages`] to an [`Iterator`] of [`Array`]
pub fn iter_to_arrays<'a, K, O, I>(
    iter: I,
    is_optional: bool,
    data_type: DataType,
    chunk_size: usize,
) -> Box<dyn Iterator<Item = Result<Arc<dyn Array>>> + 'a>
where
    I: 'a + DataPages,
    O: Offset,
    K: DictionaryKey,
{
    Box::new(
        ArrayIterator::<K, O, I>::new(iter, data_type, chunk_size, is_optional)
            .map(|x| x.map(|x| Arc::new(x) as Arc<dyn Array>)),
    )
}
