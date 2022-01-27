use std::collections::VecDeque;

use parquet2::{encoding::Encoding, page::DataPage};

use crate::{
    array::BooleanArray,
    bitmap::{utils::BitmapIter, MutableBitmap},
    datatypes::DataType,
    error::Result,
};

use super::super::utils;
use super::super::utils::{extend_from_decoder, split_buffer, Decoder, OptionalPageValidity};
use super::super::DataPages;

pub(super) fn read_required(buffer: &[u8], additional: usize, values: &mut MutableBitmap) {
    // in PLAIN, booleans are LSB bitpacked and thus we can read them as if they were a bitmap.
    values.extend_from_slice(buffer, 0, additional);
}

// The state of an optional DataPage with a boolean physical type
#[derive(Debug)]
struct Optional<'a> {
    values: BitmapIter<'a>,
    validity: OptionalPageValidity<'a>,
}

impl<'a> Optional<'a> {
    pub fn new(page: &'a DataPage) -> Self {
        let (_, validity_buffer, values_buffer, _) = split_buffer(page, page.descriptor());

        // in PLAIN, booleans are LSB bitpacked and thus we can read them as if they were a bitmap.
        // note that `values_buffer` contains only non-null values.
        // thus, at this point, it is not known how many values this buffer contains
        // values_len is the upper bound. The actual number depends on how many nulls there is.
        let values_len = values_buffer.len() * 8;
        let values = BitmapIter::new(values_buffer, 0, values_len);

        Self {
            values,
            validity: OptionalPageValidity::new(validity_buffer, page.num_values()),
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

// The state of a `DataPage` of `Boolean` parquet primitive type
#[derive(Debug)]
enum BooleanPageState<'a> {
    Optional(Optional<'a>),
    Required(Required<'a>),
}

impl<'a> BooleanPageState<'a> {
    pub fn len(&self) -> usize {
        match self {
            BooleanPageState::Optional(page) => page.validity.len(),
            BooleanPageState::Required(page) => page.length - page.offset,
        }
    }
}

impl<'a> utils::PageState<'a> for BooleanPageState<'a> {
    fn len(&self) -> usize {
        self.len()
    }
}

fn build_state(page: &DataPage, is_optional: bool) -> Result<BooleanPageState> {
    match (page.encoding(), is_optional) {
        (Encoding::Plain, true) => Ok(BooleanPageState::Optional(Optional::new(page))),
        (Encoding::Plain, false) => Ok(BooleanPageState::Required(Required::new(page))),
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

impl<'a> utils::Decoder<'a, bool, MutableBitmap> for BooleanDecoder {
    type State = BooleanPageState<'a>;
    type Array = BooleanArray;

    fn with_capacity(&self, capacity: usize) -> MutableBitmap {
        MutableBitmap::with_capacity(capacity)
    }

    fn extend_from_state(
        state: &mut Self::State,
        values: &mut MutableBitmap,
        validity: &mut MutableBitmap,
        remaining: usize,
    ) {
        match state {
            BooleanPageState::Optional(page) => extend_from_decoder(
                validity,
                &mut page.validity,
                Some(remaining),
                values,
                &mut page.values,
            ),
            BooleanPageState::Required(page) => {
                let remaining = remaining.min(page.length - page.offset);
                values.extend_from_slice(page.values, page.offset, remaining);
                page.offset += remaining;
            }
        }
    }

    fn finish(data_type: DataType, values: MutableBitmap, validity: MutableBitmap) -> Self::Array {
        BooleanArray::from_data(data_type, values.into(), validity.into())
    }
}

/// An iterator adapter over [`DataPages`] assumed to be encoded as boolean arrays
#[derive(Debug)]
pub struct BooleanArrayIterator<I: DataPages> {
    iter: I,
    data_type: DataType,
    items: VecDeque<(MutableBitmap, MutableBitmap)>,
    chunk_size: usize,
    is_optional: bool,
}

impl<I: DataPages> BooleanArrayIterator<I> {
    pub fn new(iter: I, data_type: DataType, chunk_size: usize, is_optional: bool) -> Self {
        Self {
            iter,
            data_type,
            items: VecDeque::new(),
            chunk_size,
            is_optional,
        }
    }
}

impl<I: DataPages> Iterator for BooleanArrayIterator<I> {
    type Item = Result<BooleanArray>;

    fn next(&mut self) -> Option<Self::Item> {
        // back[a1, a2, a3, ...]front
        if self.items.len() > 1 {
            return self.items.pop_back().map(|(values, validity)| {
                Ok(BooleanDecoder::finish(
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
                // there is a new page => consume the page from the start
                let maybe_page = build_state(page, self.is_optional);
                let page = match maybe_page {
                    Ok(page) => page,
                    Err(e) => return Some(Err(e)),
                };

                let maybe_array = utils::extend_from_new_page::<BooleanDecoder, _, _>(
                    page,
                    state,
                    &self.data_type,
                    self.chunk_size,
                    &mut self.items,
                    &BooleanDecoder::default(),
                );
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
                Some(Ok(BooleanDecoder::finish(
                    self.data_type.clone(),
                    values,
                    validity,
                )))
            }
        }
    }
}
