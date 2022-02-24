use std::collections::VecDeque;
use std::convert::TryInto;

use parquet2::encoding::{hybrid_rle, Encoding};
use parquet2::page::{split_buffer as _split_buffer, DataPage};
use streaming_iterator::{convert, Convert, StreamingIterator};

use crate::bitmap::utils::BitmapIter;
use crate::bitmap::MutableBitmap;
use crate::error::ArrowError;

use super::super::DataPages;

#[derive(Debug)]
pub struct BinaryIter<'a> {
    values: &'a [u8],
}

impl<'a> BinaryIter<'a> {
    pub fn new(values: &'a [u8]) -> Self {
        Self { values }
    }
}

impl<'a> Iterator for BinaryIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.values.is_empty() {
            return None;
        }
        let length = u32::from_le_bytes(self.values[0..4].try_into().unwrap()) as usize;
        self.values = &self.values[4..];
        let result = &self.values[..length];
        self.values = &self.values[length..];
        Some(result)
    }
}

pub fn not_implemented(
    encoding: &Encoding,
    is_optional: bool,
    has_dict: bool,
    version: &str,
    physical_type: &str,
) -> ArrowError {
    let required = if is_optional { "optional" } else { "required" };
    let dict = if has_dict { ", dictionary-encoded" } else { "" };
    ArrowError::NotYetImplemented(format!(
        "Decoding \"{:?}\"-encoded{} {} {} pages is not yet implemented for {}",
        encoding, dict, required, version, physical_type
    ))
}

#[inline]
pub fn split_buffer(page: &DataPage) -> (&[u8], &[u8], &[u8]) {
    _split_buffer(page, page.descriptor())
}

/// A private trait representing structs that can receive elements.
pub(super) trait Pushable<T>: Sized {
    //fn reserve(&mut self, additional: usize);
    fn push(&mut self, value: T);
    fn len(&self) -> usize;
    fn push_null(&mut self);
    fn extend_constant(&mut self, additional: usize, value: T);
}

impl Pushable<bool> for MutableBitmap {
    #[inline]
    fn len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn push(&mut self, value: bool) {
        self.push(value)
    }

    #[inline]
    fn push_null(&mut self) {
        self.push(false)
    }

    #[inline]
    fn extend_constant(&mut self, additional: usize, value: bool) {
        self.extend_constant(additional, value)
    }
}

impl<A: Copy + Default> Pushable<A> for Vec<A> {
    #[inline]
    fn len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn push_null(&mut self) {
        self.push(A::default())
    }

    #[inline]
    fn push(&mut self, value: A) {
        self.push(value)
    }

    #[inline]
    fn extend_constant(&mut self, additional: usize, value: A) {
        self.resize(self.len() + additional, value);
    }
}

#[derive(Debug)]
pub struct OptionalPageValidity<'a> {
    validity: Convert<hybrid_rle::Decoder<'a>>,
    // invariants:
    // * run_offset < length
    // * consumed < length
    run_offset: usize,
    consumed: usize,
    length: usize,
}

impl<'a> OptionalPageValidity<'a> {
    #[inline]
    pub fn new(page: &'a DataPage) -> Self {
        let (_, validity, _) = split_buffer(page);

        let validity = convert(hybrid_rle::Decoder::new(validity, 1));
        Self {
            validity,
            run_offset: 0,
            consumed: 0,
            length: page.num_values(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.length - self.consumed
    }
}

/// Extends a [`Pushable`] from an iterator of non-null values and an hybrid-rle decoder
pub(super) fn extend_from_decoder<'a, T: Default, P: Pushable<T>, I: Iterator<Item = T>>(
    validity: &mut MutableBitmap,
    page_validity: &mut OptionalPageValidity<'a>,
    limit: Option<usize>,
    values: &mut P,
    mut values_iter: I,
) {
    let limit = limit.unwrap_or(usize::MAX);

    // todo: remove `consumed_here` and compute next limit from `consumed`
    let mut consumed_here = 0;
    while consumed_here < limit {
        if page_validity.run_offset == 0 {
            page_validity.validity.advance()
        }

        if let Some(run) = page_validity.validity.get() {
            match run {
                hybrid_rle::HybridEncoded::Bitpacked(pack) => {
                    // a pack has at most `pack.len() * 8` bits
                    // during execution, we may end in the middle of a pack (run_offset != 0)
                    // the remaining items in the pack is dictacted by a combination
                    // of the page length, the offset in the pack, and where we are in the page
                    let pack_size = pack.len() * 8 - page_validity.run_offset;
                    let remaining = page_validity.length - page_validity.consumed;
                    let length = std::cmp::min(pack_size, remaining);

                    let additional = limit.min(length);

                    // consume `additional` items
                    let iter = BitmapIter::new(pack, page_validity.run_offset, additional);
                    for is_valid in iter {
                        if is_valid {
                            values.push(values_iter.next().unwrap())
                        } else {
                            values.push_null()
                        };
                    }

                    validity.extend_from_slice(pack, page_validity.run_offset, additional);

                    if additional == length {
                        page_validity.run_offset = 0
                    } else {
                        page_validity.run_offset += additional;
                    };
                    consumed_here += additional;
                    page_validity.consumed += additional;
                }
                &hybrid_rle::HybridEncoded::Rle(value, length) => {
                    let is_set = value[0] == 1;
                    let length = length - page_validity.run_offset;

                    // the number of elements that will be consumed in this (run, iteration)
                    let additional = limit.min(length);

                    validity.extend_constant(additional, is_set);
                    if is_set {
                        (0..additional).for_each(|_| values.push(values_iter.next().unwrap()));
                    } else {
                        values.extend_constant(additional, T::default());
                    }

                    if additional == length {
                        page_validity.run_offset = 0
                    } else {
                        page_validity.run_offset += additional;
                    };
                    consumed_here += additional;
                    page_validity.consumed += additional;
                }
            };
        } else {
            break;
        }
    }
}

/// The state of a partially deserialized page
pub(super) trait PageState<'a> {
    fn len(&self) -> usize;
}

/// A decoder that knows how to map `State` -> Array
pub(super) trait Decoder<'a, C: Default, P: Pushable<C>> {
    type State: PageState<'a>;

    fn build_state(&self, page: &'a DataPage) -> Result<Self::State, ArrowError>;

    /// Initializes a new pushable
    fn with_capacity(&self, capacity: usize) -> P;

    /// extends (values, validity) by deserializing items in `State`.
    /// It guarantees that the length of `values` is at most `values.len() + remaining`.
    fn extend_from_state(
        &self,
        page: &mut Self::State,
        values: &mut P,
        validity: &mut MutableBitmap,
        additional: usize,
    );
}

pub(super) fn extend_from_new_page<'a, T: Decoder<'a, C, P>, C: Default, P: Pushable<C>>(
    mut page: T::State,
    state: Option<(P, MutableBitmap)>,
    chunk_size: usize,
    items: &mut VecDeque<(P, MutableBitmap)>,
    decoder: &T,
) -> Result<Option<(P, MutableBitmap)>, ArrowError> {
    let (mut values, mut validity) = if let Some((values, validity)) = state {
        // there is a already a state => it must be incomplete...
        debug_assert!(
            values.len() < chunk_size,
            "the temp array is expected to be incomplete"
        );
        (values, validity)
    } else {
        // there is no state => initialize it
        (
            decoder.with_capacity(chunk_size),
            MutableBitmap::with_capacity(chunk_size),
        )
    };

    let remaining = chunk_size - values.len();

    // extend the current state
    decoder.extend_from_state(&mut page, &mut values, &mut validity, remaining);

    if values.len() < chunk_size {
        // the whole page was consumed and we still do not have enough items
        // => push the values to `items` so that it can be continued later
        items.push_back((values, validity));
        // and indicate that there is no item available
        return Ok(None);
    }

    while page.len() > 0 {
        let mut values = decoder.with_capacity(chunk_size);
        let mut validity = MutableBitmap::with_capacity(chunk_size);
        decoder.extend_from_state(&mut page, &mut values, &mut validity, chunk_size);
        items.push_back((values, validity))
    }

    // and return this array
    Ok(Some((values, validity)))
}

#[derive(Debug)]
pub enum MaybeNext<P> {
    Some(P),
    None,
    More,
}

#[inline]
pub(super) fn next<'a, I: DataPages, C: Default, P: Pushable<C>, D: Decoder<'a, C, P>>(
    iter: &'a mut I,
    items: &mut VecDeque<(P, MutableBitmap)>,
    chunk_size: usize,
    decoder: &D,
) -> MaybeNext<Result<(P, MutableBitmap), ArrowError>> {
    // back[a1, a2, a3, ...]front
    if items.len() > 1 {
        let item = items.pop_back().unwrap();
        return MaybeNext::Some(Ok(item));
    }
    match (items.pop_back(), iter.next()) {
        (_, Err(e)) => MaybeNext::Some(Err(e.into())),
        (None, Ok(None)) => MaybeNext::None,
        (state, Ok(Some(page))) => {
            // there is a new page => consume the page from the start
            let maybe_page = decoder.build_state(page);
            let page = match maybe_page {
                Ok(page) => page,
                Err(e) => return MaybeNext::Some(Err(e)),
            };

            let maybe_array = extend_from_new_page(page, state, chunk_size, items, decoder);

            match maybe_array {
                Ok(Some((values, validity))) => MaybeNext::Some(Ok((values, validity))),
                Ok(None) => MaybeNext::More,
                Err(e) => MaybeNext::Some(Err(e)),
            }
        }
        (Some((values, validity)), Ok(None)) => {
            // we have a populated item and no more pages
            // the only case where an item's length may be smaller than chunk_size
            debug_assert!(values.len() <= chunk_size);
            MaybeNext::Some(Ok((values, validity)))
        }
    }
}

#[inline]
pub(super) fn dict_indices_decoder(
    indices_buffer: &[u8],
    additional: usize,
) -> hybrid_rle::HybridRleDecoder {
    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    hybrid_rle::HybridRleDecoder::new(indices_buffer, bit_width as u32, additional)
}
