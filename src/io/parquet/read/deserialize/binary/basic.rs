use std::collections::VecDeque;
use std::default::Default;

use parquet2::{
    deserialize::SliceFilteredIter,
    encoding::{hybrid_rle, Encoding},
    indexes::Interval,
    page::{BinaryPageDict, DataPage},
    schema::Repetition,
};

use crate::{
    array::{Array, BinaryArray, Offset, Utf8Array},
    bitmap::{Bitmap, MutableBitmap},
    buffer::Buffer,
    datatypes::DataType,
    error::Result,
};

use super::super::utils::{
    extend_from_decoder, next, DecodedState, MaybeNext, OptionalPageValidity,
};
use super::super::DataPages;
use super::{super::utils, utils::*};

/*
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
 */

#[derive(Debug)]
pub(super) struct Required<'a> {
    pub values: SizedBinaryIter<'a>,
}

impl<'a> Required<'a> {
    pub fn new(page: &'a DataPage) -> Self {
        let values = SizedBinaryIter::new(page.buffer(), page.num_values());

        Self { values }
    }

    pub fn len(&self) -> usize {
        self.values.size_hint().0
    }
}

#[derive(Debug)]
pub(super) struct FilteredRequired<'a> {
    pub values: SliceFilteredIter<SizedBinaryIter<'a>>,
}

impl<'a> FilteredRequired<'a> {
    pub fn new(page: &'a DataPage) -> Self {
        let values = SizedBinaryIter::new(page.buffer(), page.num_values());

        let rows = page
            .selected_rows()
            .unwrap_or(&[Interval::new(0, page.num_values())])
            .iter()
            .copied()
            .collect();
        let values = SliceFilteredIter::new(values, rows);

        Self { values }
    }

    pub fn len(&self) -> usize {
        self.values.size_hint().0
    }
}

#[derive(Debug)]
pub(super) struct RequiredDictionary<'a> {
    pub values: hybrid_rle::HybridRleDecoder<'a>,
    pub dict: &'a BinaryPageDict,
}

impl<'a> RequiredDictionary<'a> {
    pub fn new(page: &'a DataPage, dict: &'a BinaryPageDict) -> Self {
        let values = utils::dict_indices_decoder(page);

        Self { dict, values }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.size_hint().0
    }
}

#[derive(Debug)]
pub(super) struct ValuesDictionary<'a> {
    pub values: hybrid_rle::HybridRleDecoder<'a>,
    pub dict: &'a BinaryPageDict,
}

impl<'a> ValuesDictionary<'a> {
    pub fn new(page: &'a DataPage, dict: &'a BinaryPageDict) -> Self {
        let values = utils::dict_indices_decoder(page);

        Self { dict, values }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.size_hint().0
    }
}

enum State<'a> {
    Optional(OptionalPageValidity<'a>, BinaryIter<'a>),
    Required(Required<'a>),
    RequiredDictionary(RequiredDictionary<'a>),
    OptionalDictionary(OptionalPageValidity<'a>, ValuesDictionary<'a>),
    FilteredRequired(FilteredRequired<'a>),
}

impl<'a> utils::PageState<'a> for State<'a> {
    fn len(&self) -> usize {
        match self {
            State::Optional(validity, _) => validity.len(),
            State::Required(state) => state.len(),
            State::RequiredDictionary(values) => values.len(),
            State::OptionalDictionary(optional, _) => optional.len(),
            State::FilteredRequired(state) => state.len(),
        }
    }
}

pub trait TraitBinaryArray<O: Offset>: Array + 'static {
    fn try_new(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Result<Self>
    where
        Self: Sized;
}

impl<O: Offset> TraitBinaryArray<O> for BinaryArray<O> {
    fn try_new(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Result<Self> {
        Self::try_new(data_type, offsets, values, validity)
    }
}

impl<O: Offset> TraitBinaryArray<O> for Utf8Array<O> {
    fn try_new(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Result<Self> {
        Self::try_new(data_type, offsets, values, validity)
    }
}

impl<'a, O: Offset> DecodedState<'a> for (Binary<O>, MutableBitmap) {
    fn len(&self) -> usize {
        self.0.len()
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
        let is_filtered = page.selected_rows().is_some();

        match (
            page.encoding(),
            page.dictionary_page(),
            is_optional,
            is_filtered,
        ) {
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false, false) => {
                Ok(State::RequiredDictionary(RequiredDictionary::new(
                    page,
                    dict.as_any().downcast_ref().unwrap(),
                )))
            }
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true, false) => {
                let dict = dict.as_any().downcast_ref().unwrap();

                Ok(State::OptionalDictionary(
                    OptionalPageValidity::new(page),
                    ValuesDictionary::new(page, dict),
                ))
            }
            (Encoding::Plain, _, true, false) => {
                let (_, _, values) = utils::split_buffer(page);

                let values = BinaryIter::new(values);

                Ok(State::Optional(OptionalPageValidity::new(page), values))
            }
            (Encoding::Plain, _, false, false) => Ok(State::Required(Required::new(page))),
            (Encoding::Plain, _, false, true) => {
                Ok(State::FilteredRequired(FilteredRequired::new(page)))
            }
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
            State::Optional(page_validity, page_values) => extend_from_decoder(
                validity,
                page_validity,
                Some(additional),
                values,
                page_values,
            ),
            State::Required(page) => {
                for x in page.values.by_ref().take(additional) {
                    values.push(x)
                }
            }
            State::FilteredRequired(page) => {
                for x in page.values.by_ref().take(additional) {
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
                utils::extend_from_decoder(
                    validity,
                    page_validity,
                    Some(additional),
                    values,
                    &mut page_values.values.by_ref().map(op),
                )
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
        }
    }
}

pub(super) fn finish<O: Offset, A: TraitBinaryArray<O>>(
    data_type: &DataType,
    values: Binary<O>,
    validity: MutableBitmap,
) -> Result<A> {
    A::try_new(
        data_type.clone(),
        values.offsets.0.into(),
        values.values.into(),
        validity.into(),
    )
}

pub struct Iter<O: Offset, A: TraitBinaryArray<O>, I: DataPages> {
    iter: I,
    data_type: DataType,
    items: VecDeque<(Binary<O>, MutableBitmap)>,
    chunk_size: usize,
    phantom_a: std::marker::PhantomData<A>,
}

impl<O: Offset, A: TraitBinaryArray<O>, I: DataPages> Iter<O, A, I> {
    pub fn new(iter: I, data_type: DataType, chunk_size: usize) -> Self {
        Self {
            iter,
            data_type,
            items: VecDeque::new(),
            chunk_size,
            phantom_a: Default::default(),
        }
    }
}

impl<O: Offset, A: TraitBinaryArray<O>, I: DataPages> Iterator for Iter<O, A, I> {
    type Item = Result<A>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_state = next(
            &mut self.iter,
            &mut self.items,
            self.chunk_size,
            &BinaryDecoder::<O>::default(),
        );
        match maybe_state {
            MaybeNext::Some(Ok((values, validity))) => {
                Some(finish(&self.data_type, values, validity))
            }
            MaybeNext::Some(Err(e)) => Some(Err(e)),
            MaybeNext::None => None,
            MaybeNext::More => self.next(),
        }
    }
}
