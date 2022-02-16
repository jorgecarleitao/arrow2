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
use super::basic::values_iter;

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

impl<'a> Decoder<'a, bool, MutableBitmap> for BooleanDecoder {
    type State = State<'a>;

    fn build_state(&self, page: &'a DataPage) -> Result<Self::State> {
        let is_optional =
            page.descriptor().type_().get_basic_info().repetition() == &Repetition::Optional;

        match (page.encoding(), is_optional) {
            (Encoding::Plain, true) => {
                let (_, _, values, _) = utils::split_buffer(page, page.descriptor());
                Ok(State::Optional(Optional::new(page), values_iter(values)))
            }
            (Encoding::Plain, false) => Ok(State::Required(Required::new(page))),
            _ => Err(utils::not_implemented(
                &page.encoding(),
                is_optional,
                false,
                "any",
                "Boolean",
            )),
        }
    }

    fn with_capacity(&self, capacity: usize) -> MutableBitmap {
        MutableBitmap::with_capacity(capacity)
    }

    fn extend_from_state(
        &self,
        state: &mut State,
        values: &mut MutableBitmap,
        validity: &mut MutableBitmap,
        required: usize,
    ) {
        match state {
            State::Optional(page_validity, page_values) => {
                let max_def = page_validity.max_def();
                read_optional_values(
                    page_validity.definition_levels.by_ref(),
                    max_def,
                    page_values.by_ref(),
                    values,
                    validity,
                    required,
                )
            }
            State::Required(page) => {
                values.extend_from_slice(page.values, page.offset, required);
                page.offset += required;
            }
        }
    }
}

/// An iterator adapter over [`DataPages`] assumed to be encoded as boolean arrays
#[derive(Debug)]
pub struct ArrayIterator<I: DataPages> {
    iter: I,
    init: InitNested,
    // invariant: items.len() == nested.len()
    items: VecDeque<(MutableBitmap, MutableBitmap)>,
    nested: VecDeque<NestedState>,
    chunk_size: usize,
}

impl<I: DataPages> ArrayIterator<I> {
    pub fn new(iter: I, init: InitNested, chunk_size: usize) -> Self {
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
    BooleanArray::from_data(data_type.clone(), values.into(), validity.into())
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
            MaybeNext::Some(Ok((nested, values, validity))) => {
                Some(Ok((nested, finish(&DataType::Boolean, values, validity))))
            }
            MaybeNext::Some(Err(e)) => Some(Err(e)),
            MaybeNext::None => None,
            MaybeNext::More => self.next(),
        }
    }
}
