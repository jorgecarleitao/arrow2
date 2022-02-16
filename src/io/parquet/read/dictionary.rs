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
    utils::{self, extend_from_decoder, Decoder, MaybeNext, OptionalPageValidity},
    DataPages,
};

// The state of a `DataPage` of `Primitive` parquet primitive type
#[derive(Debug)]
pub enum State<'a, K>
where
    K: DictionaryKey,
{
    Optional(Optional<'a, K>),
    Required(Required<'a, K>),
}

#[inline]
fn values_iter1<K>(
    indices_buffer: &[u8],
    additional: usize,
) -> std::iter::Map<HybridRleDecoder, Box<dyn Fn(u32) -> K>>
where
    K: DictionaryKey,
{
    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let new_indices = HybridRleDecoder::new(indices_buffer, bit_width as u32, additional);
    new_indices.map(Box::new(|x| K::from_u32(x).unwrap()) as _)
}

#[derive(Debug)]
pub struct Required<'a, K>
where
    K: DictionaryKey,
{
    values: std::iter::Map<HybridRleDecoder<'a>, Box<dyn Fn(u32) -> K + 'a>>,
}

impl<'a, K> Required<'a, K>
where
    K: DictionaryKey,
{
    fn new(page: &'a DataPage) -> Self {
        let (_, _, indices_buffer) = utils::split_buffer(page);

        let values = values_iter1(indices_buffer, page.num_values());

        Self { values }
    }
}

#[derive(Debug)]
pub struct Optional<'a, K>
where
    K: DictionaryKey,
{
    values: std::iter::Map<HybridRleDecoder<'a>, Box<dyn Fn(u32) -> K + 'a>>,
    validity: OptionalPageValidity<'a>,
}

impl<'a, K> Optional<'a, K>
where
    K: DictionaryKey,
{
    fn new(page: &'a DataPage) -> Self {
        let (_, _, indices_buffer) = utils::split_buffer(page);

        let values = values_iter1(indices_buffer, page.num_values());

        Self {
            values,
            validity: OptionalPageValidity::new(page),
        }
    }
}

impl<'a, K> utils::PageState<'a> for State<'a, K>
where
    K: DictionaryKey,
{
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

impl<'a, K> utils::Decoder<'a, K, Vec<K>> for PrimitiveDecoder<K>
where
    K: DictionaryKey,
{
    type State = State<'a, K>;

    fn build_state(&self, page: &'a DataPage) -> Result<Self::State> {
        let is_optional =
            page.descriptor().type_().get_basic_info().repetition() == &Repetition::Optional;

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

    fn with_capacity(&self, capacity: usize) -> Vec<K> {
        Vec::<K>::with_capacity(capacity)
    }

    fn extend_from_state(
        &self,
        state: &mut Self::State,
        values: &mut Vec<K>,
        validity: &mut MutableBitmap,
        remaining: usize,
    ) {
        match state {
            State::Optional(page) => extend_from_decoder(
                validity,
                &mut page.validity,
                Some(remaining),
                values,
                &mut page.values,
            ),
            State::Required(page) => {
                values.extend(page.values.by_ref().take(remaining));
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
        let (values, validity) = items.pop_back().unwrap();
        let keys = finish_key(values, validity);
        return MaybeNext::Some(Ok(DictionaryArray::from_data(keys, dict.unwrap())));
    }
    match (items.pop_back(), iter.next()) {
        (_, Err(e)) => MaybeNext::Some(Err(e.into())),
        (None, Ok(None)) => MaybeNext::None,
        (state, Ok(Some(page))) => {
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

            let maybe_array = {
                // there is a new page => consume the page from the start
                let maybe_page = PrimitiveDecoder::default().build_state(page);
                let page = match maybe_page {
                    Ok(page) => page,
                    Err(e) => return MaybeNext::Some(Err(e)),
                };

                utils::extend_from_new_page::<PrimitiveDecoder<K>, _, _>(
                    page,
                    state,
                    chunk_size,
                    items,
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

                    MaybeNext::Some(Ok(DictionaryArray::from_data(keys, dict.unwrap())))
                }
                Ok(None) => MaybeNext::More,
                Err(e) => MaybeNext::Some(Err(e)),
            }
        }
        (Some((values, validity)), Ok(None)) => {
            // we have a populated item and no more pages
            // the only case where an item's length may be smaller than chunk_size
            debug_assert!(values.len() <= chunk_size);

            let keys = finish_key(values, validity);

            MaybeNext::Some(Ok(DictionaryArray::from_data(keys, dict.unwrap())))
        }
    }
}
