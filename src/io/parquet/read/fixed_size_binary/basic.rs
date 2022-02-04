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
            extend_from_decoder, next, not_implemented, split_buffer, Decoder, MaybeNext,
            OptionalPageValidity, PageState,
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

#[inline]
fn values_iter1<'a>(
    indices_buffer: &'a [u8],
    dict: &'a FixedLenByteArrayPageDict,
    additional: usize,
) -> std::iter::Map<hybrid_rle::HybridRleDecoder<'a>, Box<dyn Fn(u32) -> &'a [u8] + 'a>> {
    let dict_values = dict.values();
    let size = dict.size();

    let op = Box::new(move |index: u32| {
        let index = index as usize;
        &dict_values[index * size..(index + 1) * size]
    }) as _;

    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let indices = hybrid_rle::HybridRleDecoder::new(indices_buffer, bit_width as u32, additional);
    indices.map(op)
}

struct RequiredDictionary<'a> {
    pub values: std::iter::Map<hybrid_rle::HybridRleDecoder<'a>, Box<dyn Fn(u32) -> &'a [u8] + 'a>>,
    pub remaining: usize,
}

impl<'a> RequiredDictionary<'a> {
    fn new(page: &'a DataPage, dict: &'a FixedLenByteArrayPageDict) -> Self {
        let values = values_iter1(page.buffer(), dict, page.num_values());

        Self {
            values,
            remaining: page.num_values(),
        }
    }
}

struct OptionalDictionary<'a> {
    values: std::iter::Map<hybrid_rle::HybridRleDecoder<'a>, Box<dyn Fn(u32) -> &'a [u8] + 'a>>,
    validity: OptionalPageValidity<'a>,
}

impl<'a> OptionalDictionary<'a> {
    fn new(page: &'a DataPage, dict: &'a FixedLenByteArrayPageDict) -> Self {
        let (_, _, values_buffer, _) = split_buffer(page, page.descriptor());

        let values = values_iter1(values_buffer, dict, page.num_values());

        Self {
            values,
            validity: OptionalPageValidity::new(page),
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
            State::OptionalDictionary(page) => extend_from_decoder(
                validity,
                &mut page.validity,
                Some(remaining),
                values,
                &mut page.values,
            ),
            State::RequiredDictionary(page) => {
                page.remaining -= remaining;
                for x in page.values.by_ref().take(remaining) {
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
