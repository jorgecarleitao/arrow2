use std::collections::VecDeque;

use parquet2::{
    encoding::{hybrid_rle::HybridRleDecoder, Encoding},
    page::{DataPage, DictPage},
    schema::Repetition,
};

use crate::{
    array::{Array, DictionaryArray, DictionaryKey},
    bitmap::MutableBitmap,
    error::{Error, Result},
};
use crate::{datatypes::DataType, io::parquet::read::deserialize::utils::DecodedState};

use super::{
    super::super::DataPages,
    super::nested_utils::*,
    super::utils::{dict_indices_decoder, not_implemented, Decoder, MaybeNext, PageState},
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

// The state of a `DataPage` of `Boolean` parquet boolean type
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum State<'a> {
    Optional(Optional<'a>, HybridRleDecoder<'a>),
    Required(Required<'a>),
}

impl<'a> State<'a> {
    pub fn len(&self) -> usize {
        match self {
            State::Optional(optional, _) => optional.len(),
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
pub struct PrimitiveDecoder<K>
where
    K: DictionaryKey,
{
    phantom_k: std::marker::PhantomData<K>,
}

impl<K> Default for PrimitiveDecoder<K>
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

impl<'a, K: DictionaryKey> Decoder<'a> for PrimitiveDecoder<K> {
    type State = State<'a>;
    type DecodedState = (Vec<K>, MutableBitmap);

    fn build_state(&self, page: &'a DataPage) -> Result<Self::State> {
        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;
        let is_filtered = page.selected_rows().is_some();

        match (page.encoding(), is_optional, is_filtered) {
            (Encoding::Plain, true, false) => Ok(State::Optional(
                Optional::try_new(page)?,
                dict_indices_decoder(page)?,
            )),
            (Encoding::Plain, false, false) => Required::try_new(page).map(State::Required),
            _ => Err(not_implemented(page)),
        }
    }

    fn with_capacity(&self, capacity: usize) -> Self::DecodedState {
        (
            Vec::with_capacity(capacity),
            MutableBitmap::with_capacity(capacity),
        )
    }

    fn extend_from_state(
        &self,
        state: &mut State,
        decoded: &mut Self::DecodedState,
        additional: usize,
    ) {
        let (values, validity) = decoded;
        match state {
            State::Optional(page_validity, page_values) => {
                let items = page_validity.by_ref().take(additional);
                let items = Zip::new(items, page_values.by_ref().map(|x| K::from_u32(x).unwrap()));

                read_optional_values(items, values, validity)
            }
            State::Required(page) => {
                values.extend(
                    page.values
                        .by_ref()
                        .map(|x| K::from_u32(x).unwrap())
                        .take(additional),
                );
            }
        }
    }
}

pub fn next_dict<'a, K: DictionaryKey, I: DataPages, F: Fn(&dyn DictPage) -> Box<dyn Array>>(
    iter: &'a mut I,
    items: &mut VecDeque<(Vec<K>, MutableBitmap)>,
    nested_items: &mut VecDeque<NestedState>,
    init: &[InitNested],
    dict: &mut Dict,
    data_type: DataType,
    chunk_size: Option<usize>,
    read_dict: F,
) -> MaybeNext<Result<(NestedState, DictionaryArray<K>)>> {
    if items.len() > 1 {
        let nested = nested_items.pop_front().unwrap();
        let (values, validity) = items.pop_front().unwrap();
        let keys = finish_key(values, validity);
        let dict = DictionaryArray::try_new(data_type, keys, dict.unwrap());
        return MaybeNext::Some(dict.map(|dict| (nested, dict)));
    }
    match iter.next() {
        Err(e) => MaybeNext::Some(Err(e.into())),
        Ok(None) => {
            if let Some(nested) = nested_items.pop_front() {
                // we have a populated item and no more pages
                // the only case where an item's length may be smaller than chunk_size
                let (values, validity) = items.pop_front().unwrap();
                debug_assert!(values.len() <= chunk_size.unwrap_or(usize::MAX));

                let keys = finish_key(values, validity);

                let dict = DictionaryArray::try_new(data_type, keys, dict.unwrap());
                return MaybeNext::Some(dict.map(|dict| (nested, dict)));
            } else {
                MaybeNext::None
            }
        }
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

            // there is a new page => consume the page from the start
            let mut nested_page = NestedPage::try_new(page)?;

            extend_offsets1(&mut nested_page, init, nested_items, chunk_size);

            let decoder = PrimitiveDecoder::<K>::default();

            let maybe_page = decoder.build_state(page);
            let page = match maybe_page {
                Ok(page) => page,
                Err(e) => return MaybeNext::Some(Err(e)),
            };

            extend_from_new_page(page, items, nested_items, &decoder);

            if items.front().unwrap().len() < chunk_size.unwrap_or(usize::MAX) {
                MaybeNext::More
            } else {
                let nested = nested_items.pop_front().unwrap();
                let (values, validity) = items.pop_front().unwrap();
                let keys = finish_key(values, validity);

                let dict = DictionaryArray::try_new(data_type, keys, dict.unwrap());
                return MaybeNext::Some(dict.map(|dict| (nested, dict)));
            }
        }
    }
}
