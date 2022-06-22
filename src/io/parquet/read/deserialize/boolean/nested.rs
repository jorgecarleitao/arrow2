use std::collections::VecDeque;

use parquet2::{
    encoding::Encoding,
    page::{split_buffer, DataPage},
    schema::Repetition,
};

use crate::{
    array::BooleanArray,
    bitmap::{utils::BitmapIter, MutableBitmap},
    datatypes::DataType,
    error::Result,
};

use super::super::nested_utils::*;
use super::super::utils;
use super::super::utils::MaybeNext;
use super::super::DataPages;

// The state of a `DataPage` of `Boolean` parquet boolean type
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum State<'a> {
    Optional(BitmapIter<'a>),
    Required(BitmapIter<'a>),
}

impl<'a> State<'a> {
    pub fn len(&self) -> usize {
        match self {
            State::Optional(iter) => iter.size_hint().0,
            State::Required(iter) => iter.size_hint().0,
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

impl<'a> NestedDecoder<'a> for BooleanDecoder {
    type State = State<'a>;
    type DecodedState = (MutableBitmap, MutableBitmap);

    fn build_state(&self, page: &'a DataPage) -> Result<Self::State> {
        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;
        let is_filtered = page.selected_rows().is_some();

        match (page.encoding(), is_optional, is_filtered) {
            (Encoding::Plain, true, false) => {
                let (_, _, values) = split_buffer(page)?;
                let values = BitmapIter::new(values, 0, values.len() * 8);

                Ok(State::Optional(values))
            }
            (Encoding::Plain, false, false) => {
                let (_, _, values) = split_buffer(page)?;
                let values = BitmapIter::new(values, 0, values.len() * 8);

                Ok(State::Required(values))
            }
            _ => Err(utils::not_implemented(page)),
        }
    }

    fn with_capacity(&self, capacity: usize) -> Self::DecodedState {
        (
            MutableBitmap::with_capacity(capacity),
            MutableBitmap::with_capacity(capacity),
        )
    }

    fn push_valid(&self, state: &mut State, decoded: &mut Self::DecodedState) {
        let (values, validity) = decoded;
        match state {
            State::Optional(page_values) => {
                let value = page_values.next().unwrap_or_default();
                values.push(value);
                validity.push(true);
            }
            State::Required(page_values) => {
                let value = page_values.next().unwrap_or_default();
                values.push(value);
            }
        }
    }

    fn push_null(&self, decoded: &mut Self::DecodedState) {
        let (values, validity) = decoded;
        values.push(false);
        validity.push(false);
    }
}

/// An iterator adapter over [`DataPages`] assumed to be encoded as boolean arrays
#[derive(Debug)]
pub struct NestedIter<I: DataPages> {
    iter: I,
    init: Vec<InitNested>,
    items: VecDeque<(NestedState, (MutableBitmap, MutableBitmap))>,
    chunk_size: Option<usize>,
}

impl<I: DataPages> NestedIter<I> {
    pub fn new(iter: I, init: Vec<InitNested>, chunk_size: Option<usize>) -> Self {
        Self {
            iter,
            init,
            items: VecDeque::new(),
            chunk_size,
        }
    }
}

fn finish(data_type: &DataType, values: MutableBitmap, validity: MutableBitmap) -> BooleanArray {
    BooleanArray::new(data_type.clone(), values.into(), validity.into())
}

impl<I: DataPages> Iterator for NestedIter<I> {
    type Item = Result<(NestedState, BooleanArray)>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_state = next(
            &mut self.iter,
            &mut self.items,
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

/// Converts [`DataPages`] to an [`Iterator`] of [`BooleanArray`]
pub fn iter_to_arrays_nested<'a, I: 'a>(
    iter: I,
    init: Vec<InitNested>,
    chunk_size: Option<usize>,
) -> NestedArrayIter<'a>
where
    I: DataPages,
{
    Box::new(NestedIter::new(iter, init, chunk_size).map(|result| {
        let (mut nested, array) = result?;
        let _ = nested.nested.pop().unwrap(); // the primitive
        Ok((nested, array.boxed()))
    }))
}
