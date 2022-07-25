use std::collections::VecDeque;

use parquet2::{
    encoding::{hybrid_rle::HybridRleDecoder, Encoding},
    page::{DataPage, DictPage},
    schema::Repetition,
};

use crate::datatypes::DataType;
use crate::{
    array::{Array, DictionaryArray, DictionaryKey},
    bitmap::MutableBitmap,
    error::{Error, Result},
};

use super::{
    super::super::DataPages,
    super::nested_utils::*,
    super::utils::{dict_indices_decoder, not_implemented, MaybeNext, PageState},
    finish_key, Dict,
};

// The state of a required DataPage with a boolean physical type
#[derive(Debug)]
pub struct Required<'a> {
    values: HybridRleDecoder<'a>,
    length: usize,
}

impl<'a> Required<'a> {
    fn try_new(page: &'a DataPage) -> Result<Self> {
        let values = dict_indices_decoder(page)?;
        let length = page.num_values();
        Ok(Self { values, length })
    }
}

// The state of a `DataPage` of a `Dictionary` type
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum State<'a> {
    Optional(HybridRleDecoder<'a>),
    Required(Required<'a>),
}

impl<'a> State<'a> {
    pub fn len(&self) -> usize {
        match self {
            State::Optional(page) => page.len(),
            State::Required(page) => page.length,
        }
    }
}

impl<'a> PageState<'a> for State<'a> {
    fn len(&self) -> usize {
        self.len()
    }
}

#[derive(Debug)]
pub struct DictionaryDecoder<K>
where
    K: DictionaryKey,
{
    phantom_k: std::marker::PhantomData<K>,
}

impl<K> Default for DictionaryDecoder<K>
where
    K: DictionaryKey,
{
    #[inline]
    fn default() -> Self {
        Self {
            phantom_k: std::marker::PhantomData,
        }
    }
}

impl<'a, K: DictionaryKey> NestedDecoder<'a> for DictionaryDecoder<K> {
    type State = State<'a>;
    type DecodedState = (Vec<K>, MutableBitmap);

    fn build_state(&self, page: &'a DataPage) -> Result<Self::State> {
        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;
        let is_filtered = page.selected_rows().is_some();

        match (page.encoding(), is_optional, is_filtered) {
            (Encoding::RleDictionary | Encoding::PlainDictionary, true, false) => {
                dict_indices_decoder(page).map(State::Optional)
            }
            (Encoding::RleDictionary | Encoding::PlainDictionary, false, false) => {
                Required::try_new(page).map(State::Required)
            }
            _ => Err(not_implemented(page)),
        }
    }

    fn with_capacity(&self, capacity: usize) -> Self::DecodedState {
        (
            Vec::with_capacity(capacity),
            MutableBitmap::with_capacity(capacity),
        )
    }

    fn push_valid(&self, state: &mut Self::State, decoded: &mut Self::DecodedState) {
        let (values, validity) = decoded;
        match state {
            State::Optional(page_values) => {
                let key = page_values.next();
                // todo: convert unwrap to error
                let key = match K::try_from(key.unwrap_or_default() as usize) {
                    Ok(key) => key,
                    Err(_) => todo!(),
                };
                values.push(key);
                validity.push(true);
            }
            State::Required(page_values) => {
                let key = page_values.values.next();
                let key = match K::try_from(key.unwrap_or_default() as usize) {
                    Ok(key) => key,
                    Err(_) => todo!(),
                };
                values.push(key);
            }
        }
    }

    fn push_null(&self, decoded: &mut Self::DecodedState) {
        let (values, validity) = decoded;
        values.push(K::default());
        validity.push(false)
    }
}

#[allow(clippy::too_many_arguments)]
pub fn next_dict<'a, K: DictionaryKey, I: DataPages, F: Fn(&dyn DictPage) -> Box<dyn Array>>(
    iter: &'a mut I,
    items: &mut VecDeque<(NestedState, (Vec<K>, MutableBitmap))>,
    remaining: &mut usize,
    init: &[InitNested],
    dict: &mut Dict,
    data_type: DataType,
    chunk_size: Option<usize>,
    read_dict: F,
) -> MaybeNext<Result<(NestedState, DictionaryArray<K>)>> {
    if items.len() > 1 {
        let (nested, (values, validity)) = items.pop_front().unwrap();
        let keys = finish_key(values, validity);
        let dict = DictionaryArray::try_new(data_type, keys, dict.unwrap());
        return MaybeNext::Some(dict.map(|dict| (nested, dict)));
    }
    match iter.next() {
        Err(e) => MaybeNext::Some(Err(e.into())),
        Ok(Some(page)) => {
            // consume the dictionary page
            match (&dict, page.dictionary_page()) {
                (Dict::Empty, None) => {
                    return MaybeNext::Some(Err(Error::nyi(
                        "dictionary arrays from non-dict-encoded pages",
                    )));
                }
                (Dict::Empty, Some(dict_page)) => {
                    *dict = Dict::Complete(read_dict(dict_page.as_ref()))
                }
                (Dict::Complete(_), _) => {}
            };

            let error = extend(
                page,
                init,
                items,
                remaining,
                &DictionaryDecoder::<K>::default(),
                chunk_size,
            );
            match error {
                Ok(_) => {}
                Err(e) => return MaybeNext::Some(Err(e)),
            };

            if items.front().unwrap().0.len() < chunk_size.unwrap_or(usize::MAX) {
                MaybeNext::More
            } else {
                let (nested, (values, validity)) = items.pop_front().unwrap();
                let keys = finish_key(values, validity);
                let dict = DictionaryArray::try_new(data_type, keys, dict.unwrap());
                MaybeNext::Some(dict.map(|dict| (nested, dict)))
            }
        }
        Ok(None) => {
            if let Some((nested, (values, validity))) = items.pop_front() {
                // we have a populated item and no more pages
                // the only case where an item's length may be smaller than chunk_size
                debug_assert!(values.len() <= chunk_size.unwrap_or(usize::MAX));

                let keys = finish_key(values, validity);
                let dict = DictionaryArray::try_new(data_type, keys, dict.unwrap());
                MaybeNext::Some(dict.map(|dict| (nested, dict)))
            } else {
                MaybeNext::None
            }
        }
    }
}
