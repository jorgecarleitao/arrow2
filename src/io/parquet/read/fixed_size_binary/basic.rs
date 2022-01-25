use std::collections::VecDeque;

use parquet2::{
    encoding::{hybrid_rle, Encoding},
    page::{DataPage, FixedLenByteArrayPageDict},
};
use streaming_iterator::{convert, Convert};

use crate::{
    array::FixedSizeBinaryArray,
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::Result,
    io::parquet::read::{
        utils::{
            extend_from_decoder, extend_from_new_page, not_implemented, split_buffer, Decoder,
            PageState, Pushable,
        },
        DataPages,
    },
};

use super::utils::FixedSizeBinary;

struct Optional<'a> {
    pub values: std::slice::ChunksExact<'a, u8>,
    pub validity: Convert<hybrid_rle::Decoder<'a>>,
    // invariant: offset <= length;
    pub offset: usize,
    pub length: usize,
}

impl<'a> Optional<'a> {
    fn new(page: &'a DataPage, size: usize) -> Self {
        let (_, validity_buffer, values_buffer, _) = split_buffer(page, page.descriptor());

        let values = values_buffer.chunks_exact(size);

        let validity = convert(hybrid_rle::Decoder::new(validity_buffer, 1));
        Self {
            values,
            validity,
            offset: 0,
            length: page.num_values(),
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
    pub values: std::iter::Map<hybrid_rle::HybridRleDecoder<'a>, Box<dyn Fn(u32) -> &'a [u8] + 'a>>,
    pub validity: Convert<hybrid_rle::Decoder<'a>>,
    // invariant: offset <= length;
    pub offset: usize,
    pub length: usize,
}

impl<'a> OptionalDictionary<'a> {
    fn new(page: &'a DataPage, dict: &'a FixedLenByteArrayPageDict) -> Self {
        let (_, validity_buffer, values_buffer, _) = split_buffer(page, page.descriptor());

        let values = values_iter1(values_buffer, dict, page.num_values());

        let validity = convert(hybrid_rle::Decoder::new(validity_buffer, 1));

        Self {
            values,
            validity,
            offset: 0,
            length: page.num_values(),
        }
    }
}

enum State<'a> {
    Optional(Optional<'a>),
    Required(Required<'a>),
    RequiredDictionary(RequiredDictionary<'a>),
    OptionalDictionary(OptionalDictionary<'a>),
}

fn build_state(page: &DataPage, is_optional: bool, size: usize) -> Result<State> {
    match (page.encoding(), page.dictionary_page(), is_optional) {
        (Encoding::Plain, None, true) => Ok(State::Optional(Optional::new(page, size))),
        (Encoding::Plain, None, false) => Ok(State::Required(Required::new(page, size))),
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

impl<'a> PageState<'a> for State<'a> {
    fn len(&self) -> usize {
        match self {
            State::Optional(state) => state.length - state.offset,
            State::Required(state) => state.remaining,
            State::RequiredDictionary(state) => state.remaining,
            State::OptionalDictionary(state) => state.length - state.offset,
        }
    }
}

struct BinaryDecoder {
    size: usize,
}

impl<'a> Decoder<'a, &'a [u8], FixedSizeBinary> for BinaryDecoder {
    type State = State<'a>;
    type Array = FixedSizeBinaryArray;

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
                page.length,
                &mut page.offset,
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
                page.length,
                &mut page.offset,
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

    fn finish(
        data_type: DataType,
        values: FixedSizeBinary,
        validity: MutableBitmap,
    ) -> Self::Array {
        FixedSizeBinaryArray::from_data(data_type, values.values.into(), validity.into())
    }
}

pub struct BinaryArrayIterator<I: DataPages> {
    iter: I,
    data_type: DataType,
    size: usize,
    items: VecDeque<(FixedSizeBinary, MutableBitmap)>,
    chunk_size: usize,
    is_optional: bool,
}

impl<I: DataPages> BinaryArrayIterator<I> {
    pub fn new(iter: I, data_type: DataType, chunk_size: usize, is_optional: bool) -> Self {
        let size = FixedSizeBinaryArray::get_size(&data_type);
        Self {
            iter,
            data_type,
            size,
            items: VecDeque::new(),
            chunk_size,
            is_optional,
        }
    }
}

impl<I: DataPages> Iterator for BinaryArrayIterator<I> {
    type Item = Result<FixedSizeBinaryArray>;

    fn next(&mut self) -> Option<Self::Item> {
        // back[a1, a2, a3, ...]front
        if self.items.len() > 1 {
            return self.items.pop_back().map(|(values, validity)| {
                Ok(BinaryDecoder::finish(
                    self.data_type.clone(),
                    values,
                    validity,
                ))
            });
        }
        match (self.items.pop_back(), self.iter.next()) {
            (_, Err(e)) => Some(Err(e.into())),
            (None, Ok(None)) => None,
            (state, Ok(Some(page))) => {
                let maybe_array = {
                    // there is a new page => consume the page from the start
                    let maybe_page = build_state(page, self.is_optional, self.size);
                    let page = match maybe_page {
                        Ok(page) => page,
                        Err(e) => return Some(Err(e)),
                    };

                    extend_from_new_page::<BinaryDecoder, _, _>(
                        page,
                        state,
                        &self.data_type,
                        self.chunk_size,
                        &mut self.items,
                        &BinaryDecoder { size: self.size },
                    )
                };
                match maybe_array {
                    Ok(Some(array)) => Some(Ok(array)),
                    Ok(None) => self.next(),
                    Err(e) => Some(Err(e)),
                }
            }
            (Some((values, validity)), Ok(None)) => {
                // we have a populated item and no more pages
                // the only case where an item's length may be smaller than chunk_size
                debug_assert!(values.len() <= self.chunk_size);
                Some(Ok(BinaryDecoder::finish(
                    self.data_type.clone(),
                    values,
                    validity,
                )))
            }
        }
    }
}
