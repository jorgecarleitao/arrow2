use std::{collections::VecDeque, sync::Arc};

use parquet2::{
    encoding::{hybrid_rle::HybridRleDecoder, Encoding},
    page::{DataPage, DictPage},
    schema::Repetition,
};

use crate::{
    array::{Array, DictionaryArray, DictionaryKey, PrimitiveArray},
    bitmap::MutableBitmap,
    error::{ArrowError, Result},
};

use super::{
    utils::{
        self, dict_indices_decoder, extend_from_decoder, DecodedState, Decoder, MaybeNext,
        OptionalPageValidity,
    },
    DataPages,
};

// The state of a `DataPage` of `Primitive` parquet primitive type
#[derive(Debug)]
pub enum State<'a> {
    Optional(Optional<'a>),
    Required(Required<'a>),
}

#[derive(Debug)]
pub struct Required<'a> {
    values: HybridRleDecoder<'a>,
}

impl<'a> Required<'a> {
    fn new(page: &'a DataPage) -> Self {
        let values = dict_indices_decoder(page);
        Self { values }
    }
}

#[derive(Debug)]
pub struct Optional<'a> {
    values: HybridRleDecoder<'a>,
    validity: OptionalPageValidity<'a>,
}

impl<'a> Optional<'a> {
    fn new(page: &'a DataPage) -> Self {
        let values = dict_indices_decoder(page);

        Self {
            values,
            validity: OptionalPageValidity::new(page),
        }
    }
}

impl<'a> utils::PageState<'a> for State<'a> {
    fn len(&self) -> usize {
        match self {
            State::Optional(optional) => optional.validity.len(),
            State::Required(required) => required.values.size_hint().0,
        }
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

impl<'a, K> utils::Decoder<'a> for PrimitiveDecoder<K>
where
    K: DictionaryKey,
{
    type State = State<'a>;
    type DecodedState = (Vec<K>, MutableBitmap);

    fn build_state(&self, page: &'a DataPage) -> Result<Self::State> {
        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;

        match (page.encoding(), is_optional) {
            (Encoding::PlainDictionary | Encoding::RleDictionary, false) => {
                Ok(State::Required(Required::new(page)))
            }
            (Encoding::PlainDictionary | Encoding::RleDictionary, true) => {
                Ok(State::Optional(Optional::new(page)))
            }
            _ => Err(utils::not_implemented(
                &page.encoding(),
                is_optional,
                false,
                "any",
                "Primitive",
            )),
        }
    }

    fn with_capacity(&self, capacity: usize) -> Self::DecodedState {
        (
            Vec::<K>::with_capacity(capacity),
            MutableBitmap::with_capacity(capacity),
        )
    }

    fn extend_from_state(
        &self,
        state: &mut Self::State,
        decoded: &mut Self::DecodedState,
        remaining: usize,
    ) {
        let (values, validity) = decoded;
        match state {
            State::Optional(page) => extend_from_decoder(
                validity,
                &mut page.validity,
                Some(remaining),
                values,
                &mut page.values.by_ref().map(|x| K::from_u32(x).unwrap()),
            ),
            State::Required(page) => {
                values.extend(
                    page.values
                        .by_ref()
                        .map(|x| K::from_u32(x).unwrap())
                        .take(remaining),
                );
            }
        }
    }
}

#[derive(Debug)]
pub enum Dict {
    Empty,
    Complete(Arc<dyn Array>),
}

impl Dict {
    pub fn unwrap(&self) -> Arc<dyn Array> {
        match self {
            Self::Empty => panic!(),
            Self::Complete(array) => array.clone(),
        }
    }
}

fn finish_key<K: DictionaryKey>(values: Vec<K>, validity: MutableBitmap) -> PrimitiveArray<K> {
    PrimitiveArray::from_data(K::PRIMITIVE.into(), values.into(), validity.into())
}

#[inline]
pub(super) fn next_dict<
    'a,
    K: DictionaryKey,
    I: DataPages,
    F: Fn(&dyn DictPage) -> Arc<dyn Array>,
>(
    iter: &'a mut I,
    items: &mut VecDeque<(Vec<K>, MutableBitmap)>,
    dict: &mut Dict,
    chunk_size: usize,
    read_dict: F,
) -> MaybeNext<Result<DictionaryArray<K>>> {
    if items.len() > 1 {
        let (values, validity) = items.pop_front().unwrap();
        let keys = finish_key(values, validity);
        return MaybeNext::Some(Ok(DictionaryArray::from_data(keys, dict.unwrap())));
    }
    match iter.next() {
        Err(e) => MaybeNext::Some(Err(e.into())),
        Ok(Some(page)) => {
            // consume the dictionary page
            match (&dict, page.dictionary_page()) {
                (Dict::Empty, None) => {
                    return MaybeNext::Some(Err(ArrowError::nyi(
                        "dictionary arrays from non-dict-encoded pages",
                    )));
                }
                (Dict::Empty, Some(dict_page)) => {
                    *dict = Dict::Complete(read_dict(dict_page.as_ref()))
                }
                (Dict::Complete(_), _) => {}
            };

            // there is a new page => consume the page from the start
            let maybe_page = PrimitiveDecoder::<K>::default().build_state(page);
            let page = match maybe_page {
                Ok(page) => page,
                Err(e) => return MaybeNext::Some(Err(e)),
            };

            utils::extend_from_new_page(page, chunk_size, items, &PrimitiveDecoder::<K>::default());

            if items.front().unwrap().len() < chunk_size {
                MaybeNext::More
            } else {
                let (values, validity) = items.pop_front().unwrap();
                let keys =
                    PrimitiveArray::from_data(K::PRIMITIVE.into(), values.into(), validity.into());
                MaybeNext::Some(Ok(DictionaryArray::from_data(keys, dict.unwrap())))
            }
        }
        Ok(None) => {
            if let Some((values, validity)) = items.pop_front() {
                // we have a populated item and no more pages
                // the only case where an item's length may be smaller than chunk_size
                debug_assert!(values.len() <= chunk_size);

                let keys = finish_key(values, validity);

                MaybeNext::Some(Ok(DictionaryArray::from_data(keys, dict.unwrap())))
            } else {
                MaybeNext::None
            }
        }
    }
}
