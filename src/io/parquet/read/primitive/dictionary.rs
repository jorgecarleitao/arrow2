use std::{collections::VecDeque, sync::Arc};

use parquet2::{
    encoding::{hybrid_rle::HybridRleDecoder, Encoding},
    page::{DataPage, PrimitivePageDict},
    types::NativeType as ParquetNativeType,
};

use super::super::utils;
use crate::io::parquet::read::utils::Decoder;
use crate::{
    array::{Array, DictionaryArray, DictionaryKey, PrimitiveArray},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::{ArrowError, Result},
    io::parquet::read::{
        utils::{extend_from_decoder, OptionalPageValidity},
        DataPages,
    },
    types::NativeType,
};

// The state of a `DataPage` of `Primitive` parquet primitive type
#[derive(Debug)]
enum State<'a, K>
where
    K: DictionaryKey,
{
    Optional(Optional<'a, K>),
    //Required(Required<'a, T, P, F>),
}

#[inline]
pub fn values_iter1<K>(
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
struct Optional<'a, K>
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

fn build_state<K>(page: &DataPage, is_optional: bool) -> Result<State<K>>
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
struct PrimitiveDecoder<K>
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
    type Array = PrimitiveArray<K>;

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

    fn finish(data_type: DataType, values: Vec<K>, validity: MutableBitmap) -> Self::Array {
        let data_type = match data_type {
            DataType::Dictionary(_, values, _) => values.as_ref().clone(),
            _ => data_type,
        };
        PrimitiveArray::from_data(data_type, values.into(), validity.into())
    }
}

#[derive(Debug)]
enum Dict {
    Empty,
    Complete(Arc<dyn Array>),
}

impl Dict {
    fn unwrap(&self) -> Arc<dyn Array> {
        match self {
            Self::Empty => panic!(),
            Self::Complete(array) => array.clone(),
        }
    }
}

/// An iterator adapter over [`DataPages`] assumed to be encoded as boolean arrays
#[derive(Debug)]
pub struct PrimitiveArrayIterator<K, T, I, P, F>
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
    is_optional: bool,
    op: F,
    phantom: std::marker::PhantomData<P>,
}

impl<K, T, I, P, F> PrimitiveArrayIterator<K, T, I, P, F>
where
    K: DictionaryKey,
    I: DataPages,
    T: NativeType,

    P: ParquetNativeType,
    F: Copy + Fn(P) -> T,
{
    fn new(iter: I, data_type: DataType, chunk_size: usize, is_optional: bool, op: F) -> Self {
        Self {
            iter,
            data_type,
            values: Dict::Empty,
            items: VecDeque::new(),
            chunk_size,
            is_optional,
            op,
            phantom: Default::default(),
        }
    }
}

impl<K, T, I, P, F> Iterator for PrimitiveArrayIterator<K, T, I, P, F>
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
                                T::PRIMITIVE.into(),
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
pub fn iter_to_arrays<'a, K, I, T, P, F>(
    iter: I,
    is_optional: bool,
    data_type: DataType,
    chunk_size: usize,
    op: F,
) -> Box<dyn Iterator<Item = Result<Arc<dyn Array>>> + 'a>
where
    I: 'a + DataPages,
    K: DictionaryKey,
    T: NativeType,
    P: ParquetNativeType,
    F: 'a + Copy + Fn(P) -> T,
{
    Box::new(
        PrimitiveArrayIterator::<K, T, I, P, F>::new(iter, data_type, chunk_size, is_optional, op)
            .map(|x| x.map(|x| Arc::new(x) as Arc<dyn Array>)),
    )
}
