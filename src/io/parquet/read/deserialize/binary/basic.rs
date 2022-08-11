use std::collections::VecDeque;
use std::default::Default;

use parquet2::{
    deserialize::SliceFilteredIter,
    encoding::{hybrid_rle, Encoding},
    page::{split_buffer, DataPage, DictPage},
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
    extend_from_decoder, get_selected_rows, next, DecodedState, FilteredOptionalPageValidity,
    MaybeNext, OptionalPageValidity,
};
use super::super::Pages;
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
    pub fn try_new(page: &'a DataPage) -> Result<Self> {
        let (_, _, values) = split_buffer(page)?;
        let values = SizedBinaryIter::new(values, page.num_values());

        Ok(Self { values })
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

        let rows = get_selected_rows(page);
        let values = SliceFilteredIter::new(values, rows);

        Self { values }
    }

    pub fn len(&self) -> usize {
        self.values.size_hint().0
    }
}

pub(super) type Dict = Vec<Vec<u8>>;

#[derive(Debug)]
pub(super) struct RequiredDictionary<'a> {
    pub values: hybrid_rle::HybridRleDecoder<'a>,
    pub dict: &'a Dict,
}

impl<'a> RequiredDictionary<'a> {
    pub fn try_new(page: &'a DataPage, dict: &'a Dict) -> Result<Self> {
        let values = utils::dict_indices_decoder(page)?;

        Ok(Self { dict, values })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.size_hint().0
    }
}

#[derive(Debug)]
pub(super) struct FilteredRequiredDictionary<'a> {
    pub values: SliceFilteredIter<hybrid_rle::HybridRleDecoder<'a>>,
    pub dict: &'a Dict,
}

impl<'a> FilteredRequiredDictionary<'a> {
    pub fn try_new(page: &'a DataPage, dict: &'a Dict) -> Result<Self> {
        let values = utils::dict_indices_decoder(page)?;

        let rows = get_selected_rows(page);
        let values = SliceFilteredIter::new(values, rows);

        Ok(Self { values, dict })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.size_hint().0
    }
}

#[derive(Debug)]
pub(super) struct ValuesDictionary<'a> {
    pub values: hybrid_rle::HybridRleDecoder<'a>,
    pub dict: &'a Dict,
}

impl<'a> ValuesDictionary<'a> {
    pub fn try_new(page: &'a DataPage, dict: &'a Dict) -> Result<Self> {
        let values = utils::dict_indices_decoder(page)?;

        Ok(Self { dict, values })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.size_hint().0
    }
}

#[derive(Debug)]
enum State<'a> {
    Optional(OptionalPageValidity<'a>, BinaryIter<'a>),
    Required(Required<'a>),
    RequiredDictionary(RequiredDictionary<'a>),
    OptionalDictionary(OptionalPageValidity<'a>, ValuesDictionary<'a>),
    FilteredRequired(FilteredRequired<'a>),
    FilteredOptional(FilteredOptionalPageValidity<'a>, BinaryIter<'a>),
    FilteredRequiredDictionary(FilteredRequiredDictionary<'a>),
    FilteredOptionalDictionary(FilteredOptionalPageValidity<'a>, ValuesDictionary<'a>),
}

impl<'a> utils::PageState<'a> for State<'a> {
    fn len(&self) -> usize {
        match self {
            State::Optional(validity, _) => validity.len(),
            State::Required(state) => state.len(),
            State::RequiredDictionary(values) => values.len(),
            State::OptionalDictionary(optional, _) => optional.len(),
            State::FilteredRequired(state) => state.len(),
            State::FilteredOptional(validity, _) => validity.len(),
            State::FilteredRequiredDictionary(values) => values.len(),
            State::FilteredOptionalDictionary(optional, _) => optional.len(),
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

impl<O: Offset> DecodedState for (Binary<O>, MutableBitmap) {
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
    type Dict = Dict;
    type DecodedState = (Binary<O>, MutableBitmap);

    fn build_state(&self, page: &'a DataPage, dict: Option<&'a Self::Dict>) -> Result<Self::State> {
        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;
        let is_filtered = page.selected_rows().is_some();

        match (page.encoding(), dict, is_optional, is_filtered) {
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false, false) => Ok(
                State::RequiredDictionary(RequiredDictionary::try_new(page, dict)?),
            ),
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true, false) => {
                Ok(State::OptionalDictionary(
                    OptionalPageValidity::try_new(page)?,
                    ValuesDictionary::try_new(page, dict)?,
                ))
            }
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false, true) => {
                FilteredRequiredDictionary::try_new(page, dict)
                    .map(State::FilteredRequiredDictionary)
            }
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true, true) => {
                Ok(State::FilteredOptionalDictionary(
                    FilteredOptionalPageValidity::try_new(page)?,
                    ValuesDictionary::try_new(page, dict)?,
                ))
            }
            (Encoding::Plain, _, true, false) => {
                let (_, _, values) = split_buffer(page)?;

                let values = BinaryIter::new(values);

                Ok(State::Optional(
                    OptionalPageValidity::try_new(page)?,
                    values,
                ))
            }
            (Encoding::Plain, _, false, false) => Ok(State::Required(Required::try_new(page)?)),
            (Encoding::Plain, _, false, true) => {
                Ok(State::FilteredRequired(FilteredRequired::new(page)))
            }
            (Encoding::Plain, _, true, true) => {
                let (_, _, values) = split_buffer(page)?;

                Ok(State::FilteredOptional(
                    FilteredOptionalPageValidity::try_new(page)?,
                    BinaryIter::new(values),
                ))
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
                let page_dict = &page_values.dict;
                let op = move |index: u32| page_dict[index as usize].as_ref();
                utils::extend_from_decoder(
                    validity,
                    page_validity,
                    Some(additional),
                    values,
                    &mut page_values.values.by_ref().map(op),
                )
            }
            State::RequiredDictionary(page) => {
                let page_dict = &page.dict;
                let op = move |index: u32| page_dict[index as usize].as_ref();

                for x in page.values.by_ref().map(op).take(additional) {
                    values.push(x)
                }
            }
            State::FilteredOptional(page_validity, page_values) => {
                utils::extend_from_decoder(
                    validity,
                    page_validity,
                    Some(additional),
                    values,
                    page_values.by_ref(),
                );
            }
            State::FilteredRequiredDictionary(page) => {
                let page_dict = &page.dict;
                let op = move |index: u32| page_dict[index as usize].as_ref();

                for x in page.values.by_ref().map(op).take(additional) {
                    values.push(x)
                }
            }
            State::FilteredOptionalDictionary(page_validity, page_values) => {
                let page_dict = &page_values.dict;
                let op = move |index: u32| page_dict[index as usize].as_ref();
                utils::extend_from_decoder(
                    validity,
                    page_validity,
                    Some(additional),
                    values,
                    &mut page_values.values.by_ref().map(op),
                )
            }
        }
    }

    fn deserialize_dict(&self, page: &DictPage) -> Self::Dict {
        deserialize_plain(&page.buffer, page.num_values)
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

pub struct Iter<O: Offset, A: TraitBinaryArray<O>, I: Pages> {
    iter: I,
    data_type: DataType,
    items: VecDeque<(Binary<O>, MutableBitmap)>,
    dict: Option<Dict>,
    chunk_size: Option<usize>,
    remaining: usize,
    phantom_a: std::marker::PhantomData<A>,
}

impl<O: Offset, A: TraitBinaryArray<O>, I: Pages> Iter<O, A, I> {
    pub fn new(iter: I, data_type: DataType, chunk_size: Option<usize>, num_rows: usize) -> Self {
        Self {
            iter,
            data_type,
            items: VecDeque::new(),
            dict: None,
            chunk_size,
            remaining: num_rows,
            phantom_a: Default::default(),
        }
    }
}

impl<O: Offset, A: TraitBinaryArray<O>, I: Pages> Iterator for Iter<O, A, I> {
    type Item = Result<A>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_state = next(
            &mut self.iter,
            &mut self.items,
            &mut self.dict,
            &mut self.remaining,
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

pub(super) fn deserialize_plain(values: &[u8], num_values: usize) -> Dict {
    SizedBinaryIter::new(values, num_values)
        .map(|x| x.to_vec())
        .collect()
}
