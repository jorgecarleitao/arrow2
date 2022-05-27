use std::collections::VecDeque;

use parquet2::{encoding::Encoding, page::DataPage, schema::Repetition};

use crate::{
    array::BooleanArray,
    bitmap::{utils::BitmapIter, MutableBitmap},
    datatypes::DataType,
    error::Result,
};

use super::super::nested_utils::*;
use super::super::utils;
use super::super::utils::{Decoder, MaybeNext};
use super::super::DataPages;

// The state of a required DataPage with a boolean physical type
#[derive(Debug)]
struct Required<'a> {
    values: &'a [u8],
    // invariant: offset <= length;
    offset: usize,
    length: usize,
}

impl<'a> Required<'a> {
    pub fn new(page: &'a DataPage) -> Self {
        Self {
            values: page.buffer(),
            offset: 0,
            length: page.num_values(),
        }
    }
}

// The state of a `DataPage` of `Boolean` parquet boolean type
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum State<'a> {
    Optional(Optional<'a>, BitmapIter<'a>),
    Required(Required<'a>),
}

impl<'a> State<'a> {
    pub fn len(&self) -> usize {
        match self {
            State::Optional(optional, _) => optional.len(),
            State::Required(page) => page.length - page.offset,
        }
    }
}

impl<'a> utils::PageState<'a> for State<'a> {
    fn len(&self) -> usize {
        self.len()
    }
}

#[derive(Default)]
struct BooleanDecoder {}

impl<'a> Decoder<'a> for BooleanDecoder {
    type State = State<'a>;
    type DecodedState = (MutableBitmap, MutableBitmap);

    fn build_state(&self, page: &'a DataPage) -> Result<Self::State> {
        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;
        let is_filtered = page.selected_rows().is_some();

        match (page.encoding(), is_optional, is_filtered) {
            (Encoding::Plain, true, false) => {
                let (_, _, values) = utils::split_buffer(page);
                let values = BitmapIter::new(values, 0, values.len() * 8);

                Ok(State::Optional(Optional::new(page), values))
            }
            (Encoding::Plain, false, false) => Ok(State::Required(Required::new(page))),
            _ => Err(utils::not_implemented(page)),
        }
    }

    fn with_capacity(&self, capacity: usize) -> Self::DecodedState {
        (
            MutableBitmap::with_capacity(capacity),
            MutableBitmap::with_capacity(capacity),
        )
    }

    fn extend_from_state(
        &self,
        state: &mut State,
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
                values.extend_from_slice(page.values, page.offset, additional);
                page.offset += additional;
            }
        }
    }
}

/// An iterator adapter over [`DataPages`] assumed to be encoded as boolean arrays
#[derive(Debug)]
pub struct ArrayIterator<I: DataPages> {
    iter: I,
    init: Vec<InitNested>,
    // invariant: items.len() == nested.len()
    items: VecDeque<(MutableBitmap, MutableBitmap)>,
    nested: VecDeque<NestedState>,
    chunk_size: usize,
}

impl<I: DataPages> ArrayIterator<I> {
    pub fn new(iter: I, init: Vec<InitNested>, chunk_size: usize) -> Self {
        Self {
            iter,
            init,
            items: VecDeque::new(),
            nested: VecDeque::new(),
            chunk_size,
        }
    }
}

fn finish(data_type: &DataType, values: MutableBitmap, validity: MutableBitmap) -> BooleanArray {
    BooleanArray::new(data_type.clone(), values.into(), validity.into())
}

impl<I: DataPages> Iterator for ArrayIterator<I> {
    type Item = Result<(NestedState, BooleanArray)>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_state = next(
            &mut self.iter,
            &mut self.items,
            &mut self.nested,
            &self.init,
            self.chunk_size,
            &BooleanDecoder::default(),
        );
        match maybe_state {
            MaybeNext::Some(Ok((nested, (values, validity)))) => {
                Some(Ok((nested, finish(&DataType::Boolean, values, validity))))
            }
            MaybeNext::Some(Err(e)) => Some(Err(e)),
            MaybeNext::None => None,
            MaybeNext::More => self.next(),
        }
    }
}
