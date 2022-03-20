use std::collections::VecDeque;

use parquet2::{encoding::Encoding, page::DataPage, schema::Repetition};

use crate::{
    array::BooleanArray,
    bitmap::{utils::BitmapIter, MutableBitmap},
    datatypes::DataType,
    error::Result,
};

use super::super::utils;
use super::super::utils::{
    extend_from_decoder, next, split_buffer, DecodedState, Decoder, MaybeNext, OptionalPageValidity,
};
use super::super::DataPages;

// The state of an optional DataPage with a boolean physical type
#[derive(Debug)]
struct Optional<'a> {
    values: BitmapIter<'a>,
    validity: OptionalPageValidity<'a>,
}

impl<'a> Optional<'a> {
    pub fn new(page: &'a DataPage) -> Self {
        let (_, _, values_buffer) = split_buffer(page);

        Self {
            values: BitmapIter::new(values_buffer, 0, values_buffer.len() * 8),
            validity: OptionalPageValidity::new(page),
        }
    }
}

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
#[derive(Debug)]
enum State<'a> {
    Optional(Optional<'a>),
    Required(Required<'a>),
}

impl<'a> State<'a> {
    pub fn len(&self) -> usize {
        match self {
            State::Optional(page) => page.validity.len(),
            State::Required(page) => page.length - page.offset,
        }
    }
}

impl<'a> utils::PageState<'a> for State<'a> {
    fn len(&self) -> usize {
        self.len()
    }
}

impl<'a> DecodedState<'a> for (MutableBitmap, MutableBitmap) {
    fn len(&self) -> usize {
        self.0.len()
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

        match (page.encoding(), is_optional) {
            (Encoding::Plain, true) => Ok(State::Optional(Optional::new(page))),
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

    fn with_capacity(&self, capacity: usize) -> Self::DecodedState {
        (
            MutableBitmap::with_capacity(capacity),
            MutableBitmap::with_capacity(capacity),
        )
    }

    fn extend_from_state(
        &self,
        state: &mut Self::State,
        decoded: &mut Self::DecodedState,
        remaining: usize,
    ) {
        let (values, validity) = decoded;
        match state {
            State::Optional(page) => extend_from_decoder(
                validity,
                &mut page.validity,
                Some(remaining),
                values,
                &mut page.values,
            ),
            State::Required(page) => {
                let remaining = remaining.min(page.length - page.offset);
                values.extend_from_slice(page.values, page.offset, remaining);
                page.offset += remaining;
            }
        }
    }
}

fn finish(data_type: &DataType, values: MutableBitmap, validity: MutableBitmap) -> BooleanArray {
    BooleanArray::new(data_type.clone(), values.into(), validity.into())
}

/// An iterator adapter over [`DataPages`] assumed to be encoded as boolean arrays
#[derive(Debug)]
pub struct Iter<I: DataPages> {
    iter: I,
    data_type: DataType,
    items: VecDeque<(MutableBitmap, MutableBitmap)>,
    chunk_size: usize,
}

impl<I: DataPages> Iter<I> {
    pub fn new(iter: I, data_type: DataType, chunk_size: usize) -> Self {
        Self {
            iter,
            data_type,
            items: VecDeque::new(),
            chunk_size,
        }
    }
}

impl<I: DataPages> Iterator for Iter<I> {
    type Item = Result<BooleanArray>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_state = next(
            &mut self.iter,
            &mut self.items,
            self.chunk_size,
            &BooleanDecoder::default(),
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
