use std::collections::VecDeque;

use parquet2::{encoding::Encoding, page::DataPage, schema::Repetition};

use crate::{
    array::Offset,
    bitmap::MutableBitmap,
    datatypes::{DataType, Field},
    error::Result,
    io::parquet::read::{utils::MaybeNext, DataPages},
};

use super::super::nested_utils::*;
use super::utils::Binary;
use super::{
    super::utils,
    basic::{finish, Required, TraitBinaryArray},
};

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum State<'a> {
    Optional(Optional<'a>, utils::BinaryIter<'a>),
    Required(Required<'a>),
}

impl<'a> utils::PageState<'a> for State<'a> {
    fn len(&self) -> usize {
        match self {
            State::Optional(validity, _) => validity.len(),
            State::Required(state) => state.remaining,
        }
    }
}

#[derive(Debug, Default)]
struct BinaryDecoder<O: Offset> {
    phantom_o: std::marker::PhantomData<O>,
}

impl<'a, O: Offset> utils::Decoder<'a, &'a [u8], Binary<O>> for BinaryDecoder<O> {
    type State = State<'a>;

    fn build_state(&self, page: &'a DataPage) -> Result<Self::State> {
        let is_optional =
            page.descriptor().type_().get_basic_info().repetition() == &Repetition::Optional;

        match (page.encoding(), page.dictionary_page(), is_optional) {
            (Encoding::Plain, None, true) => {
                let (_, _, values, _) = utils::split_buffer(page, page.descriptor());

                let values = utils::BinaryIter::new(values);

                Ok(State::Optional(Optional::new(page), values))
            }
            (Encoding::Plain, None, false) => Ok(State::Required(Required::new(page))),
            _ => Err(utils::not_implemented(
                &page.encoding(),
                is_optional,
                false,
                "any",
                "Binary",
            )),
        }
    }

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
            State::Optional(page_validity, page_values) => {
                let max_def = page_validity.max_def();
                read_optional_values(
                    page_validity.definition_levels.by_ref(),
                    max_def,
                    page_values.by_ref(),
                    values,
                    validity,
                    additional,
                )
            }
            State::Required(page) => {
                page.remaining -= additional;
                for x in page.values.by_ref().take(additional) {
                    values.push(x)
                }
            }
        }
    }
}

pub struct ArrayIterator<O: Offset, A: TraitBinaryArray<O>, I: DataPages> {
    iter: I,
    data_type: DataType,
    field: Field,
    items: VecDeque<(Binary<O>, MutableBitmap)>,
    nested: VecDeque<NestedState>,
    chunk_size: usize,
    phantom_a: std::marker::PhantomData<A>,
}

impl<O: Offset, A: TraitBinaryArray<O>, I: DataPages> ArrayIterator<O, A, I> {
    pub fn new(iter: I, field: Field, data_type: DataType, chunk_size: usize) -> Self {
        Self {
            iter,
            data_type,
            field,
            items: VecDeque::new(),
            nested: VecDeque::new(),
            chunk_size,
            phantom_a: Default::default(),
        }
    }
}

impl<O: Offset, A: TraitBinaryArray<O>, I: DataPages> Iterator for ArrayIterator<O, A, I> {
    type Item = Result<(NestedState, A)>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_state = next(
            &mut self.iter,
            &mut self.items,
            &mut self.nested,
            &self.field,
            self.chunk_size,
            &BinaryDecoder::<O>::default(),
        );
        match maybe_state {
            MaybeNext::Some(Ok((nested, values, validity))) => {
                Some(Ok((nested, finish(&self.data_type, values, validity))))
            }
            MaybeNext::Some(Err(e)) => Some(Err(e)),
            MaybeNext::None => None,
            MaybeNext::More => self.next(),
        }
    }
}
