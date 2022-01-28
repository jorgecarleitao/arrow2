use std::sync::Arc;

use parquet2::{
    encoding::{hybrid_rle::HybridRleDecoder, Encoding},
    page::DataPage,
};

use super::utils;
use crate::{
    array::{Array, DictionaryKey, PrimitiveArray},
    bitmap::MutableBitmap,
    error::Result,
    io::parquet::read::utils::{extend_from_decoder, OptionalPageValidity},
};

// The state of a `DataPage` of `Primitive` parquet primitive type
#[derive(Debug)]
pub enum State<'a, K>
where
    K: DictionaryKey,
{
    Optional(Optional<'a, K>),
    //Required(Required<'a, T, P, F>),
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
        let (_, validity_buffer, indices_buffer, _) = utils::split_buffer(page, page.descriptor());

        let values = values_iter1(indices_buffer, page.num_values());

        Self {
            values,
            validity: OptionalPageValidity::new(validity_buffer, page.num_values()),
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
        }
    }
}

pub fn build_state<K>(page: &DataPage, is_optional: bool) -> Result<State<K>>
where
    K: DictionaryKey,
{
    match (page.encoding(), is_optional) {
        (Encoding::PlainDictionary | Encoding::RleDictionary, false) => {
            todo!()
            /*Ok(State::Required(
                RequiredDictionaryPage::new(page, dict, op2),
            ))*/
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

    fn with_capacity(&self, capacity: usize) -> Vec<K> {
        Vec::<K>::with_capacity(capacity)
    }

    fn extend_from_state(
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
            /*State::Required(page) => {
                values.extend(page.values.by_ref().take(remaining));
            }*/
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

pub fn finish_key<K: DictionaryKey>(values: Vec<K>, validity: MutableBitmap) -> PrimitiveArray<K> {
    PrimitiveArray::from_data(K::PRIMITIVE.into(), values.into(), validity.into())
}
