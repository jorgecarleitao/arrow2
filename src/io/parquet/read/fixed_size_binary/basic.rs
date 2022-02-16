use std::collections::VecDeque;

use parquet2::{
    encoding::{hybrid_rle, Encoding},
    page::{DataPage, FixedLenByteArrayPageDict},
    schema::Repetition,
};

use crate::{
    array::FixedSizeBinaryArray,
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::Result,
    io::parquet::read::{
        utils::{
            dict_indices_decoder, extend_from_decoder, next, not_implemented, split_buffer,
            Decoder, MaybeNext, OptionalPageValidity, PageState,
        },
        DataPages,
    },
};

use super::utils::FixedSizeBinary;

struct Optional<'a> {
    values: std::slice::ChunksExact<'a, u8>,
    validity: OptionalPageValidity<'a>,
}

impl<'a> Optional<'a> {
    fn new(page: &'a DataPage, size: usize) -> Self {
        let (_, _, values_buffer, _) = split_buffer(page, page.descriptor());

        let values = values_buffer.chunks_exact(size);

        Self {
            values,
            validity: OptionalPageValidity::new(page),
        }
    }
}

struct Required<'a> {
    pub values: std::slice::ChunksExact<'a, u8>,
    pub remaining: usize,
}

impl<'a> Required<'a> {
    fn new(page: &'a DataPage, size: usize) -> Self {
        Self {
            values: page.buffer().chunks_exact(size),
            remaining: page.num_values(),
        }
    }
}

struct RequiredDictionary<'a> {
    pub values: hybrid_rle::HybridRleDecoder<'a>,
    pub remaining: usize,
    dict: &'a FixedLenByteArrayPageDict,
}

impl<'a> RequiredDictionary<'a> {
    fn new(page: &'a DataPage, dict: &'a FixedLenByteArrayPageDict) -> Self {
        let values = dict_indices_decoder(page.buffer(), page.num_values());

        Self {
            values,
            remaining: page.num_values(),
            dict,
        }
    }
}

struct OptionalDictionary<'a> {
    values: hybrid_rle::HybridRleDecoder<'a>,
    validity: OptionalPageValidity<'a>,
    dict: &'a FixedLenByteArrayPageDict,
}

impl<'a> OptionalDictionary<'a> {
    fn new(page: &'a DataPage, dict: &'a FixedLenByteArrayPageDict) -> Self {
        let (_, _, indices_buffer, _) = split_buffer(page, page.descriptor());

        let values = dict_indices_decoder(indices_buffer, page.num_values());

        Self {
            values,
            validity: OptionalPageValidity::new(page),
            dict,
        }
    }
}

enum State<'a> {
    Optional(Optional<'a>),
    Required(Required<'a>),
    RequiredDictionary(RequiredDictionary<'a>),
    OptionalDictionary(OptionalDictionary<'a>),
}

impl<'a> PageState<'a> for State<'a> {
    fn len(&self) -> usize {
        match self {
            State::Optional(state) => state.validity.len(),
            State::Required(state) => state.remaining,
            State::RequiredDictionary(state) => state.remaining,
            State::OptionalDictionary(state) => state.validity.len(),
        }
    }
}

struct BinaryDecoder {
    size: usize,
}

impl<'a> Decoder<'a, &'a [u8], FixedSizeBinary> for BinaryDecoder {
    type State = State<'a>;

    fn build_state(&self, page: &'a DataPage) -> Result<Self::State> {
        let is_optional =
            page.descriptor().type_().get_basic_info().repetition() == &Repetition::Optional;

        match (page.encoding(), page.dictionary_page(), is_optional) {
            (Encoding::Plain, None, true) => Ok(State::Optional(Optional::new(page, self.size))),
            (Encoding::Plain, None, false) => Ok(State::Required(Required::new(page, self.size))),
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false) => {
                Ok(State::RequiredDictionary(RequiredDictionary::new(
                    page,
                    dict.as_any().downcast_ref().unwrap(),
                )))
            }
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true) => {
                Ok(State::OptionalDictionary(OptionalDictionary::new(
                    page,
                    dict.as_any().downcast_ref().unwrap(),
                )))
            }
            _ => Err(not_implemented(
                &page.encoding(),
                is_optional,
                false,
                "any",
                "FixedBinary",
            )),
        }
    }

    fn with_capacity(&self, capacity: usize) -> FixedSizeBinary {
        FixedSizeBinary::with_capacity(capacity, self.size)
    }

    fn extend_from_state(
        &self,
        state: &mut Self::State,
        values: &mut FixedSizeBinary,
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
                page.remaining -= remaining;
                for x in page.values.by_ref().take(remaining) {
                    values.push(x)
                }
            }
            State::OptionalDictionary(page) => {
                let dict_values = page.dict.values();
                let size = page.dict.size();
                let op = |index: u32| {
                    let index = index as usize;
                    &dict_values[index * size..(index + 1) * size]
                };

                extend_from_decoder(
                    validity,
                    &mut page.validity,
                    Some(remaining),
                    values,
                    page.values.by_ref().map(op),
                )
            }
            State::RequiredDictionary(page) => {
                let dict_values = page.dict.values();
                let size = page.dict.size();
                let op = |index: u32| {
                    let index = index as usize;
                    &dict_values[index * size..(index + 1) * size]
                };

                page.remaining -= remaining;
                for x in page.values.by_ref().map(op).take(remaining) {
                    values.push(x)
                }
            }
        }
    }
}

fn finish(
    data_type: &DataType,
    values: FixedSizeBinary,
    validity: MutableBitmap,
) -> FixedSizeBinaryArray {
    FixedSizeBinaryArray::from_data(data_type.clone(), values.values.into(), validity.into())
}

pub struct Iter<I: DataPages> {
    iter: I,
    data_type: DataType,
    size: usize,
    items: VecDeque<(FixedSizeBinary, MutableBitmap)>,
    chunk_size: usize,
}

impl<I: DataPages> Iter<I> {
    pub fn new(iter: I, data_type: DataType, chunk_size: usize) -> Self {
        let size = FixedSizeBinaryArray::get_size(&data_type);
        Self {
            iter,
            data_type,
            size,
            items: VecDeque::new(),
            chunk_size,
        }
    }
}

impl<I: DataPages> Iterator for Iter<I> {
    type Item = Result<FixedSizeBinaryArray>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_state = next(
            &mut self.iter,
            &mut self.items,
            self.chunk_size,
            &BinaryDecoder { size: self.size },
        );
        match maybe_state {
            MaybeNext::Some(Ok((values, validity))) => {
                Some(Ok(finish(&self.data_type, values, validity)))
            }
            MaybeNext::Some(Err(e)) => Some(Err(e)),
            MaybeNext::None => None,
            MaybeNext::More => self.next(),
        }
    }
}
