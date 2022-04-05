use std::collections::VecDeque;

use parquet2::{encoding::Encoding, page::DataPage, schema::Repetition};

use crate::{
    array::Offset, bitmap::MutableBitmap, datatypes::DataType, error::Result,
    io::parquet::read::DataPages,
};

use super::super::nested_utils::*;
use super::super::utils::MaybeNext;
use super::basic::ValuesDictionary;
use super::utils::Binary;
use super::{
    super::utils,
    basic::{finish, Required, TraitBinaryArray},
};

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum State<'a> {
    Optional(Optional<'a>, utils::BinaryIter<'a>),
    Required(Required<'a>),
    RequiredDictionary(ValuesDictionary<'a>),
    OptionalDictionary(Optional<'a>, ValuesDictionary<'a>),
}

impl<'a> utils::PageState<'a> for State<'a> {
    fn len(&self) -> usize {
        match self {
            State::Optional(validity, _) => validity.len(),
            State::Required(state) => state.remaining,
            State::RequiredDictionary(required) => required.len(),
            State::OptionalDictionary(optional, _) => optional.len(),
        }
    }
}

#[derive(Debug, Default)]
struct BinaryDecoder<O: Offset> {
    phantom_o: std::marker::PhantomData<O>,
}

impl<'a, O: Offset> utils::Decoder<'a> for BinaryDecoder<O> {
    type State = State<'a>;
    type DecodedState = (Binary<O>, MutableBitmap);

    fn build_state(&self, page: &'a DataPage) -> Result<Self::State> {
        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;

        match (page.encoding(), page.dictionary_page(), is_optional) {
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false) => {
                let dict = dict.as_any().downcast_ref().unwrap();
                Ok(State::RequiredDictionary(ValuesDictionary::new(page, dict)))
            }
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true) => {
                let dict = dict.as_any().downcast_ref().unwrap();
                Ok(State::OptionalDictionary(
                    Optional::new(page),
                    ValuesDictionary::new(page, dict),
                ))
            }
            (Encoding::Plain, None, true) => {
                let (_, _, values) = utils::split_buffer(page);

                let values = utils::BinaryIter::new(values);

                Ok(State::Optional(Optional::new(page), values))
            }
            (Encoding::Plain, None, false) => Ok(State::Required(Required::new(page))),
            _ => Err(utils::not_implemented(page)),
        }
    }

    fn with_capacity(&self, capacity: usize) -> Self::DecodedState {
        (
            Binary::<O>::with_capacity(capacity),
            MutableBitmap::with_capacity(capacity),
        )
    }

    fn extend_from_state(
        &self,
        state: &mut Self::State,
        decoded: &mut Self::DecodedState,
        additional: usize,
    ) {
        let (values, validity) = decoded;
        match state {
            State::Optional(page_validity, page_values) => {
                let items = page_validity.by_ref().take(additional);
                let items = Zip::new(items, page_values.by_ref());

                read_optional_values(items, values, validity)
            }
            State::Required(page) => {
                page.remaining -= additional;
                for x in page.values.by_ref().take(additional) {
                    values.push(x)
                }
            }
            State::RequiredDictionary(page) => {
                let dict_values = page.dict.values();
                let dict_offsets = page.dict.offsets();

                let op = move |index: u32| {
                    let index = index as usize;
                    let dict_offset_i = dict_offsets[index] as usize;
                    let dict_offset_ip1 = dict_offsets[index + 1] as usize;
                    &dict_values[dict_offset_i..dict_offset_ip1]
                };
                for x in page.values.by_ref().map(op).take(additional) {
                    values.push(x)
                }
            }
            State::OptionalDictionary(page_validity, page_values) => {
                let dict_values = page_values.dict.values();
                let dict_offsets = page_values.dict.offsets();

                let op = move |index: u32| {
                    let index = index as usize;
                    let dict_offset_i = dict_offsets[index] as usize;
                    let dict_offset_ip1 = dict_offsets[index + 1] as usize;
                    &dict_values[dict_offset_i..dict_offset_ip1]
                };

                let items = page_validity.by_ref().take(additional);
                let items = Zip::new(items, page_values.values.by_ref().map(op));

                read_optional_values(items, values, validity)
            }
        }
    }
}

pub struct ArrayIterator<O: Offset, A: TraitBinaryArray<O>, I: DataPages> {
    iter: I,
    data_type: DataType,
    init: InitNested,
    items: VecDeque<(Binary<O>, MutableBitmap)>,
    nested: VecDeque<NestedState>,
    chunk_size: usize,
    phantom_a: std::marker::PhantomData<A>,
}

impl<O: Offset, A: TraitBinaryArray<O>, I: DataPages> ArrayIterator<O, A, I> {
    pub fn new(iter: I, init: InitNested, data_type: DataType, chunk_size: usize) -> Self {
        Self {
            iter,
            data_type,
            init,
            items: VecDeque::new(),
            nested: VecDeque::new(),
            chunk_size,
            phantom_a: Default::default(),
        }
    }
}

impl<O: Offset, A: TraitBinaryArray<O>, I: DataPages> Iterator for ArrayIterator<O, A, I> {
    type Item = Result<(NestedState, A)>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_state = next(
            &mut self.iter,
            &mut self.items,
            &mut self.nested,
            &self.init,
            self.chunk_size,
            &BinaryDecoder::<O>::default(),
        );
        match maybe_state {
            MaybeNext::Some(Ok((nested, decoded))) => {
                Some(finish(&self.data_type, decoded.0, decoded.1).map(|array| (nested, array)))
            }
            MaybeNext::Some(Err(e)) => Some(Err(e)),
            MaybeNext::None => None,
            MaybeNext::More => self.next(),
        }
    }
}
