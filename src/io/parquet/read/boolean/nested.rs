use std::collections::VecDeque;

use parquet2::{
    encoding::{hybrid_rle::HybridRleDecoder, Encoding},
    page::DataPage,
    read::levels::get_bit_width,
    schema::Repetition,
};

use crate::{
    array::BooleanArray,
    bitmap::{utils::BitmapIter, MutableBitmap},
    datatypes::{DataType, Field},
    error::Result,
    io::parquet::read::{utils::Decoder, DataPages},
};

use super::super::nested_utils::*;
use super::super::utils;

// The state of an optional DataPage with a boolean physical type
#[derive(Debug)]
struct Optional<'a> {
    values: BitmapIter<'a>,
    definition_levels: HybridRleDecoder<'a>,
    max_def: u32,
}

impl<'a> Optional<'a> {
    pub fn new(page: &'a DataPage) -> Self {
        let (_, def_levels, values_buffer, _) = utils::split_buffer(page, page.descriptor());

        // in PLAIN, booleans are LSB bitpacked and thus we can read them as if they were a bitmap.
        // note that `values_buffer` contains only non-null values.
        // thus, at this point, it is not known how many values this buffer contains
        // values_len is the upper bound. The actual number depends on how many nulls there is.
        let values_len = values_buffer.len() * 8;
        let values = BitmapIter::new(values_buffer, 0, values_len);

        let max_def = page.descriptor().max_def_level();

        Self {
            values,
            definition_levels: HybridRleDecoder::new(
                def_levels,
                get_bit_width(max_def),
                page.num_values(),
            ),
            max_def: max_def as u32,
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
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum State<'a> {
    Optional(Optional<'a>),
    Required(Required<'a>),
}

impl<'a> State<'a> {
    pub fn len(&self) -> usize {
        match self {
            State::Optional(page) => page.definition_levels.size_hint().0,
            State::Required(page) => page.length - page.offset,
        }
    }
}

impl<'a> utils::PageState<'a> for State<'a> {
    fn len(&self) -> usize {
        self.len()
    }
}

fn build_state(page: &DataPage) -> Result<State> {
    let is_optional =
        page.descriptor().type_().get_basic_info().repetition() == &Repetition::Optional;

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

#[derive(Default)]
struct BooleanDecoder {}

impl<'a> Decoder<'a, bool, MutableBitmap> for BooleanDecoder {
    type State = State<'a>;

    fn with_capacity(&self, capacity: usize) -> MutableBitmap {
        MutableBitmap::with_capacity(capacity)
    }

    fn extend_from_state(
        state: &mut State,
        values: &mut MutableBitmap,
        validity: &mut MutableBitmap,
        required: usize,
    ) {
        match state {
            State::Optional(page) => read_optional_values(
                page.definition_levels.by_ref(),
                page.max_def,
                page.values.by_ref(),
                values,
                validity,
                required,
            ),
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
    field: Field,
    // invariant: items.len() == nested.len()
    items: VecDeque<(MutableBitmap, MutableBitmap)>,
    nested: VecDeque<NestedState>,
    chunk_size: usize,
}

impl<I: DataPages> ArrayIterator<I> {
    pub fn new(iter: I, field: Field, chunk_size: usize) -> Self {
        Self {
            iter,
            field,
            items: VecDeque::new(),
            nested: VecDeque::new(),
            chunk_size,
        }
    }
}

impl<I: DataPages> Iterator for ArrayIterator<I> {
    type Item = Result<(NestedState, BooleanArray)>;

    fn next(&mut self) -> Option<Self::Item> {
        // back[a1, a2, a3, ...]front
        if self.items.len() > 1 {
            let nested = self.nested.pop_back().unwrap();
            let (values, validity) = self.items.pop_back().unwrap();
            let array = BooleanArray::from_data(DataType::Boolean, values.into(), validity.into());
            return Some(Ok((nested, array)));
        }
        match (
            self.nested.pop_back(),
            self.items.pop_back(),
            self.iter.next(),
        ) {
            (_, _, Err(e)) => Some(Err(e.into())),
            (None, None, Ok(None)) => None,
            (state, p_state, Ok(Some(page))) => {
                // the invariant
                assert_eq!(state.is_some(), p_state.is_some());

                // there is a new page => consume the page from the start
                let mut nested_page = NestedPage::new(page);

                // read next chunk from `nested_page` and get number of values to read
                let maybe_nested = extend_offsets1(
                    &mut nested_page,
                    state,
                    &self.field,
                    &mut self.nested,
                    self.chunk_size,
                );
                let nested = match maybe_nested {
                    Ok(nested) => nested,
                    Err(e) => return Some(Err(e)),
                };
                // at this point we know whether there were enough rows in `page`
                // to fill chunk_size or not (`nested.is_some()`)
                // irrespectively, we need to consume the values from the page

                let maybe_page = build_state(page);
                let page = match maybe_page {
                    Ok(page) => page,
                    Err(e) => return Some(Err(e)),
                };

                let maybe_array = extend_from_new_page::<BooleanDecoder, _, _>(
                    page,
                    p_state,
                    &mut self.items,
                    &nested,
                    &self.nested,
                    &BooleanDecoder::default(),
                );
                let state = match maybe_array {
                    Ok(s) => s,
                    Err(e) => return Some(Err(e)),
                };
                match nested {
                    Some(p_state) => Some(Ok((
                        p_state,
                        BooleanArray::from_data(DataType::Boolean, state.0.into(), state.1.into()),
                    ))),
                    None => self.next(),
                }
            }
            (Some(nested), Some((values, validity)), Ok(None)) => {
                // we have a populated item and no more pages
                // the only case where an item's length may be smaller than chunk_size
                let array =
                    BooleanArray::from_data(DataType::Boolean, values.into(), validity.into());
                Some(Ok((nested, array)))
            }
            (Some(_), None, _) => unreachable!(),
            (None, Some(_), _) => unreachable!(),
        }
    }
}
