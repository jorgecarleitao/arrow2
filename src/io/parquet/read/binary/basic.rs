use std::collections::VecDeque;
use std::default::Default;

use parquet2::{
    encoding::{delta_length_byte_array, hybrid_rle, Encoding},
    metadata::ColumnDescriptor,
    page::{BinaryPageDict, DataPage},
};

use crate::{
    array::{Array, BinaryArray, Offset, Utf8Array},
    bitmap::{Bitmap, MutableBitmap},
    buffer::Buffer,
    datatypes::DataType,
    error::Result,
    io::parquet::read::{
        utils::{extend_from_decoder, Decoder, OptionalPageValidity, Pushable},
        DataPages,
    },
};

use super::{super::utils, utils::Binary};

#[inline]
fn values_iter<'a>(
    indices_buffer: &'a [u8],
    dict: &'a BinaryPageDict,
    additional: usize,
) -> impl Iterator<Item = &'a [u8]> + 'a {
    let dict_values = dict.values();
    let dict_offsets = dict.offsets();

    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let indices = hybrid_rle::HybridRleDecoder::new(indices_buffer, bit_width as u32, additional);
    indices.map(move |index| {
        let index = index as usize;
        let dict_offset_i = dict_offsets[index] as usize;
        let dict_offset_ip1 = dict_offsets[index + 1] as usize;
        &dict_values[dict_offset_i..dict_offset_ip1]
    })
}

/// Assumptions: No rep levels
#[allow(clippy::too_many_arguments)]
fn read_dict_buffer<O: Offset>(
    validity_buffer: &[u8],
    indices_buffer: &[u8],
    additional: usize,
    dict: &BinaryPageDict,
    values: &mut Binary<O>,
    validity: &mut MutableBitmap,
) {
    let length = values.len() + additional;

    let values_iter = values_iter(indices_buffer, dict, additional);

    let mut page_validity = OptionalPageValidity::new(validity_buffer, additional);

    extend_from_decoder(validity, &mut page_validity, None, values, values_iter);
}

#[allow(clippy::too_many_arguments)]
fn read_dict_required<O: Offset>(
    indices_buffer: &[u8],
    additional: usize,
    dict: &BinaryPageDict,
    values: &mut Binary<O>,
    validity: &mut MutableBitmap,
) {
    debug_assert_eq!(0, validity.len());
    let values_iterator = values_iter(indices_buffer, dict, additional);
    for value in values_iterator {
        values.push(value);
    }
}

fn read_delta_optional<O: Offset>(
    validity_buffer: &[u8],
    values_buffer: &[u8],
    additional: usize,
    values: &mut Binary<O>,
    validity: &mut MutableBitmap,
) {
    let Binary {
        offsets,
        values,
        last_offset,
    } = values;

    // values_buffer: first 4 bytes are len, remaining is values
    let mut values_iterator = delta_length_byte_array::Decoder::new(values_buffer);
    let offsets_iterator = values_iterator.by_ref().map(|x| {
        *last_offset += O::from_usize(x as usize).unwrap();
        *last_offset
    });

    let mut page_validity = OptionalPageValidity::new(validity_buffer, additional);

    // offsets:
    extend_from_decoder(
        validity,
        &mut page_validity,
        None,
        offsets,
        offsets_iterator,
    );

    // values:
    let new_values = values_iterator.into_values();
    values.extend_from_slice(new_values);
}

fn read_plain_optional<O: Offset>(
    validity_buffer: &[u8],
    values_buffer: &[u8],
    additional: usize,
    values: &mut Binary<O>,
    validity: &mut MutableBitmap,
) {
    // values_buffer: first 4 bytes are len, remaining is values
    let values_iter = utils::BinaryIter::new(values_buffer);

    let mut page_validity = OptionalPageValidity::new(validity_buffer, additional);

    extend_from_decoder(validity, &mut page_validity, None, values, values_iter)
}

pub(super) fn read_plain_required<O: Offset>(
    buffer: &[u8],
    additional: usize,
    values: &mut Binary<O>,
) {
    let values_iterator = utils::BinaryIter::new(buffer);

    // each value occupies 4 bytes + len declared in 4 bytes => reserve accordingly.
    values.offsets.reserve(additional);
    values.values.reserve(buffer.len() - 4 * additional);
    let a = values.values.capacity();
    for value in values_iterator {
        values.push(value);
    }
    debug_assert_eq!(a, values.values.capacity());
}

pub(super) fn extend_from_page<O: Offset>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    values: &mut Binary<O>,
    validity: &mut MutableBitmap,
) -> Result<()> {
    let additional = page.num_values();
    assert_eq!(descriptor.max_rep_level(), 0);
    assert!(descriptor.max_def_level() <= 1);
    let is_optional = descriptor.max_def_level() == 1;

    let (_, validity_buffer, values_buffer, version) = utils::split_buffer(page, descriptor);

    match (&page.encoding(), page.dictionary_page(), is_optional) {
        (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true) => {
            read_dict_buffer::<O>(
                validity_buffer,
                values_buffer,
                additional,
                dict.as_any().downcast_ref().unwrap(),
                values,
                validity,
            )
        }
        (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false) => {
            read_dict_required::<O>(
                values_buffer,
                additional,
                dict.as_any().downcast_ref().unwrap(),
                values,
                validity,
            )
        }
        (Encoding::DeltaLengthByteArray, None, true) => {
            read_delta_optional::<O>(validity_buffer, values_buffer, additional, values, validity)
        }
        (Encoding::Plain, _, true) => {
            read_plain_optional::<O>(validity_buffer, values_buffer, additional, values, validity)
        }
        (Encoding::Plain, _, false) => {
            read_plain_required::<O>(page.buffer(), page.num_values(), values)
        }
        _ => {
            return Err(utils::not_implemented(
                &page.encoding(),
                is_optional,
                page.dictionary_page().is_some(),
                version,
                "Binary",
            ))
        }
    };
    Ok(())
}

struct Optional<'a> {
    values: utils::BinaryIter<'a>,
    validity: OptionalPageValidity<'a>,
}

impl<'a> Optional<'a> {
    fn new(page: &'a DataPage) -> Self {
        let (_, validity_buffer, values_buffer, _) = utils::split_buffer(page, page.descriptor());

        let values = utils::BinaryIter::new(values_buffer);

        Self {
            values,
            validity: OptionalPageValidity::new(validity_buffer, page.num_values()),
        }
    }
}

struct Required<'a> {
    pub values: utils::BinaryIter<'a>,
    pub remaining: usize,
}

impl<'a> Required<'a> {
    fn new(page: &'a DataPage) -> Self {
        Self {
            values: utils::BinaryIter::new(page.buffer()),
            remaining: page.num_values(),
        }
    }
}

#[inline]
fn values_iter1<'a>(
    indices_buffer: &'a [u8],
    dict: &'a BinaryPageDict,
    additional: usize,
) -> std::iter::Map<hybrid_rle::HybridRleDecoder<'a>, Box<dyn Fn(u32) -> &'a [u8] + 'a>> {
    let dict_values = dict.values();
    let dict_offsets = dict.offsets();

    let op = Box::new(move |index: u32| {
        let index = index as usize;
        let dict_offset_i = dict_offsets[index] as usize;
        let dict_offset_ip1 = dict_offsets[index + 1] as usize;
        &dict_values[dict_offset_i..dict_offset_ip1]
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
    fn new(page: &'a DataPage, dict: &'a BinaryPageDict) -> Self {
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
    fn new(page: &'a DataPage, dict: &'a BinaryPageDict) -> Self {
        let (_, validity_buffer, values_buffer, _) = utils::split_buffer(page, page.descriptor());

        let values = values_iter1(values_buffer, dict, page.num_values());

        Self {
            values,
            validity: OptionalPageValidity::new(validity_buffer, page.num_values()),
        }
    }
}

enum State<'a> {
    Optional(Optional<'a>),
    Required(Required<'a>),
    RequiredDictionary(RequiredDictionary<'a>),
    OptionalDictionary(OptionalDictionary<'a>),
}

fn build_state<O>(page: &DataPage, is_optional: bool) -> Result<State> {
    match (page.encoding(), page.dictionary_page(), is_optional) {
        (Encoding::Plain, None, true) => Ok(State::Optional(Optional::new(page))),
        (Encoding::Plain, None, false) => Ok(State::Required(Required::new(page))),
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
        _ => Err(utils::not_implemented(
            &page.encoding(),
            is_optional,
            false,
            "any",
            "Binary",
        )),
    }
}

impl<'a> utils::PageState<'a> for State<'a> {
    fn len(&self) -> usize {
        match self {
            State::Optional(state) => state.validity.len(),
            State::Required(state) => state.remaining,
            State::RequiredDictionary(state) => state.remaining,
            State::OptionalDictionary(state) => state.validity.len(),
        }
    }
}

pub trait TraitBinaryArray<O: Offset>: Array + 'static {
    fn from_data(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Self;
}

impl<O: Offset> TraitBinaryArray<O> for BinaryArray<O> {
    fn from_data(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Self {
        Self::from_data(data_type, offsets, values, validity)
    }
}

impl<O: Offset> TraitBinaryArray<O> for Utf8Array<O> {
    fn from_data(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Self {
        Self::from_data(data_type, offsets, values, validity)
    }
}

#[derive(Debug)]
struct BinaryDecoder<O: Offset, A: TraitBinaryArray<O>> {
    phantom_o: std::marker::PhantomData<O>,
    phantom_a: std::marker::PhantomData<A>,
}

impl<O: Offset, A: TraitBinaryArray<O>> Default for BinaryDecoder<O, A> {
    #[inline]
    fn default() -> Self {
        Self {
            phantom_o: std::marker::PhantomData,
            phantom_a: std::marker::PhantomData,
        }
    }
}

impl<'a, O: Offset, A: TraitBinaryArray<O>> utils::Decoder<'a, &'a [u8], Binary<O>>
    for BinaryDecoder<O, A>
{
    type State = State<'a>;
    type Array = A;

    fn with_capacity(&self, capacity: usize) -> Binary<O> {
        Binary::<O>::with_capacity(capacity)
    }

    fn extend_from_state(
        state: &mut Self::State,
        values: &mut Binary<O>,
        validity: &mut MutableBitmap,
        additional: usize,
    ) {
        match state {
            State::Optional(page) => extend_from_decoder(
                validity,
                &mut page.validity,
                Some(additional),
                values,
                &mut page.values,
            ),
            State::Required(page) => {
                page.remaining -= additional;
                for x in page.values.by_ref().take(additional) {
                    values.push(x)
                }
            }
            State::OptionalDictionary(page) => extend_from_decoder(
                validity,
                &mut page.validity,
                Some(additional),
                values,
                &mut page.values,
            ),
            State::RequiredDictionary(page) => {
                page.remaining -= additional;
                for x in page.values.by_ref().take(additional) {
                    values.push(x)
                }
            }
        }
    }

    fn finish(data_type: DataType, values: Binary<O>, validity: MutableBitmap) -> Self::Array {
        A::from_data(
            data_type,
            values.offsets.0.into(),
            values.values.into(),
            validity.into(),
        )
    }
}

pub struct BinaryArrayIterator<O: Offset, A: TraitBinaryArray<O>, I: DataPages> {
    iter: I,
    data_type: DataType,
    items: VecDeque<(Binary<O>, MutableBitmap)>,
    chunk_size: usize,
    is_optional: bool,
    phantom_a: std::marker::PhantomData<A>,
}

impl<O: Offset, A: TraitBinaryArray<O>, I: DataPages> BinaryArrayIterator<O, A, I> {
    pub fn new(iter: I, data_type: DataType, chunk_size: usize, is_optional: bool) -> Self {
        Self {
            iter,
            data_type,
            items: VecDeque::new(),
            chunk_size,
            is_optional,
            phantom_a: Default::default(),
        }
    }
}

impl<O: Offset, A: TraitBinaryArray<O>, I: DataPages> Iterator for BinaryArrayIterator<O, A, I> {
    type Item = Result<A>;

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
                    let maybe_page = build_state::<O>(page, self.is_optional);
                    let page = match maybe_page {
                        Ok(page) => page,
                        Err(e) => return Some(Err(e)),
                    };

                    utils::extend_from_new_page::<BinaryDecoder<O, A>, _, _>(
                        page,
                        state,
                        &self.data_type,
                        self.chunk_size,
                        &mut self.items,
                        &BinaryDecoder::<O, A>::default(),
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
