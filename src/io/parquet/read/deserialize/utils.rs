use std::collections::VecDeque;

use parquet2::deserialize::{
    FilteredHybridEncoded, FilteredHybridRleDecoderIter, HybridDecoderBitmapIter, HybridEncoded,
};
use parquet2::encoding::hybrid_rle;
use parquet2::indexes::Interval;
use parquet2::page::{split_buffer as _split_buffer, DataPage};
use parquet2::schema::Repetition;

use crate::bitmap::utils::BitmapIter;
use crate::bitmap::MutableBitmap;
use crate::error::ArrowError;

use super::super::DataPages;

pub fn not_implemented(page: &DataPage) -> ArrowError {
    let is_optional = page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;
    let is_filtered = page.selected_rows().is_some();
    let required = if is_optional { "optional" } else { "required" };
    let is_filtered = if is_filtered { ", index-filtered" } else { "" };
    let dict = if page.dictionary_page().is_some() {
        ", dictionary-encoded"
    } else {
        ""
    };
    ArrowError::NotYetImplemented(format!(
        "Decoding {:?} \"{:?}\"-encoded{} {} {} parquet pages",
        page.descriptor.primitive_type.physical_type,
        page.encoding(),
        dict,
        required,
        is_filtered,
    ))
}

#[inline]
pub fn split_buffer(page: &DataPage) -> (&[u8], &[u8], &[u8]) {
    _split_buffer(page)
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

/// The state of a partially deserialized page
pub(super) trait PageValidity<'a> {
    fn next_limited(&mut self, limit: usize) -> Option<FilteredHybridEncoded<'a>>;
}

#[derive(Debug, Clone)]
pub struct FilteredOptionalPageValidity<'a> {
    iter: FilteredHybridRleDecoderIter<'a>,
    current: Option<(FilteredHybridEncoded<'a>, usize)>,
}

impl<'a> FilteredOptionalPageValidity<'a> {
    pub fn new(page: &'a DataPage) -> Self {
        let (_, validity, _) = split_buffer(page);

        let iter = hybrid_rle::Decoder::new(validity, 1);
        let iter = HybridDecoderBitmapIter::new(iter, page.num_values());
        let selected_rows = get_selected_rows(page);
        let iter = FilteredHybridRleDecoderIter::new(iter, selected_rows);

        Self {
            iter,
            current: None,
        }
    }

    pub fn len(&self) -> usize {
        self.iter.len()
    }
}

pub fn get_selected_rows(page: &DataPage) -> VecDeque<Interval> {
    page.selected_rows()
        .unwrap_or(&[Interval::new(0, page.num_values())])
        .iter()
        .copied()
        .collect()
}

impl<'a> PageValidity<'a> for FilteredOptionalPageValidity<'a> {
    fn next_limited(&mut self, limit: usize) -> Option<FilteredHybridEncoded<'a>> {
        let (run, own_offset) = if let Some((run, offset)) = self.current {
            (run, offset)
        } else {
            // a new run
            let run = self.iter.next()?; // no run -> None
            self.current = Some((run, 0));
            return self.next_limited(limit);
        };

        match run {
            FilteredHybridEncoded::Bitmap {
                values,
                offset,
                length,
            } => {
                let run_length = length - own_offset;

                let length = limit.min(run_length);

                if length == run_length {
                    self.current = None;
                } else {
                    self.current = Some((run, own_offset + length));
                }

                Some(FilteredHybridEncoded::Bitmap {
                    values,
                    offset,
                    length,
                })
            }
            FilteredHybridEncoded::Repeated { is_set, length } => {
                let run_length = length - own_offset;

                let length = limit.min(run_length);

                if length == run_length {
                    self.current = None;
                } else {
                    self.current = Some((run, own_offset + length));
                }

                Some(FilteredHybridEncoded::Repeated { is_set, length })
            }
            FilteredHybridEncoded::Skipped(set) => {
                self.current = None;
                Some(FilteredHybridEncoded::Skipped(set))
            }
        }
    }
}

pub struct Zip<V, I> {
    validity: V,
    values: I,
}

impl<V, I> Zip<V, I> {
    pub fn new(validity: V, values: I) -> Self {
        Self { validity, values }
    }
}

impl<T, V: Iterator<Item = bool>, I: Iterator<Item = T>> Iterator for Zip<V, I> {
    type Item = Option<T>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.validity
            .next()
            .map(|x| if x { self.values.next() } else { None })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.validity.size_hint()
    }
}

#[derive(Debug, Clone)]
pub struct OptionalPageValidity<'a> {
    iter: HybridDecoderBitmapIter<'a>,
    current: Option<(HybridEncoded<'a>, usize)>,
}

impl<'a> OptionalPageValidity<'a> {
    pub fn new(page: &'a DataPage) -> Self {
        let (_, validity, _) = split_buffer(page);

        let iter = hybrid_rle::Decoder::new(validity, 1);
        let iter = HybridDecoderBitmapIter::new(iter, page.num_values());
        Self {
            iter,
            current: None,
        }
    }

    pub fn len(&self) -> usize {
        self.iter.len()
            + self
                .current
                .as_ref()
                .map(|(run, offset)| run.len() - offset)
                .unwrap_or_default()
    }

    fn next_limited(&mut self, limit: usize) -> Option<FilteredHybridEncoded<'a>> {
        let (run, offset) = if let Some((run, offset)) = self.current {
            (run, offset)
        } else {
            // a new run
            let run = self.iter.next()?; // no run -> None
            self.current = Some((run, 0));
            return self.next_limited(limit);
        };

        match run {
            HybridEncoded::Bitmap(values, length) => {
                let run_length = length - offset;

                let length = limit.min(run_length);

                if length == run_length {
                    self.current = None;
                } else {
                    self.current = Some((run, offset + length));
                }

                Some(FilteredHybridEncoded::Bitmap {
                    values,
                    offset,
                    length,
                })
            }
            HybridEncoded::Repeated(is_set, run_length) => {
                let run_length = run_length - offset;

                let length = limit.min(run_length);

                if length == run_length {
                    self.current = None;
                } else {
                    self.current = Some((run, offset + length));
                }

                Some(FilteredHybridEncoded::Repeated { is_set, length })
            }
        }
    }
}

impl<'a> PageValidity<'a> for OptionalPageValidity<'a> {
    fn next_limited(&mut self, limit: usize) -> Option<FilteredHybridEncoded<'a>> {
        self.next_limited(limit)
    }
}

/// Extends a [`Pushable`] from an iterator of non-null values and an hybrid-rle decoder
pub(super) fn extend_from_decoder<'a, T: Default, P: Pushable<T>, I: Iterator<Item = T>>(
    validity: &mut MutableBitmap,
    page_validity: &mut dyn PageValidity<'a>,
    limit: Option<usize>,
    pushable: &mut P,
    mut values_iter: I,
) {
    let limit = limit.unwrap_or(usize::MAX);

    // todo: remove `consumed_here` and compute next limit from `consumed`
    let mut consumed_here = 0;
    while consumed_here < limit {
        let run = page_validity.next_limited(limit);
        let run = if let Some(run) = run { run } else { break };

        match run {
            FilteredHybridEncoded::Bitmap {
                values,
                offset,
                length,
            } => {
                // consume `length` items
                let iter = BitmapIter::new(values, offset, length);
                let iter = Zip::new(iter, &mut values_iter);

                for item in iter {
                    if let Some(item) = item {
                        pushable.push(item)
                    } else {
                        pushable.push_null()
                    }
                }
                validity.extend_from_slice(values, offset, length);

                consumed_here += length;
            }
            FilteredHybridEncoded::Repeated { is_set, length } => {
                validity.extend_constant(length, is_set);
                if is_set {
                    (0..length).for_each(|_| pushable.push(values_iter.next().unwrap()));
                } else {
                    pushable.extend_constant(length, T::default());
                }

                consumed_here += length;
            }
            FilteredHybridEncoded::Skipped(valids) => for _ in values_iter.by_ref().take(valids) {},
        };
    }
}

/// The state of a partially deserialized page
pub(super) trait PageState<'a>: std::fmt::Debug {
    fn len(&self) -> usize;
}

/// The state of a partially deserialized page
pub(super) trait DecodedState<'a> {
    fn len(&self) -> usize;
}

/// A decoder that knows how to map `State` -> Array
pub(super) trait Decoder<'a> {
    type State: PageState<'a>;
    type DecodedState: DecodedState<'a>;

    fn build_state(&self, page: &'a DataPage) -> Result<Self::State, ArrowError>;

    /// Initializes a new state
    fn with_capacity(&self, capacity: usize) -> Self::DecodedState;

    /// extends (values, validity) by deserializing items in `State`.
    /// It guarantees that the length of `values` is at most `values.len() + remaining`.
    fn extend_from_state(
        &self,
        page: &mut Self::State,
        decoded: &mut Self::DecodedState,
        additional: usize,
    );
}

pub(super) fn extend_from_new_page<'a, T: Decoder<'a>>(
    mut page: T::State,
    chunk_size: usize,
    items: &mut VecDeque<T::DecodedState>,
    decoder: &T,
) {
    let mut decoded = if let Some(decoded) = items.pop_back() {
        // there is a already a state => it must be incomplete...
        debug_assert!(
            decoded.len() <= chunk_size,
            "the temp state is expected to be incomplete"
        );
        decoded
    } else {
        // there is no state => initialize it
        decoder.with_capacity(chunk_size)
    };

    let remaining = chunk_size - decoded.len();

    // extend the current state
    decoder.extend_from_state(&mut page, &mut decoded, remaining);

    items.push_back(decoded);

    while page.len() > 0 {
        let mut decoded = decoder.with_capacity(chunk_size);
        decoder.extend_from_state(&mut page, &mut decoded, chunk_size);
        items.push_back(decoded)
    }
}

#[derive(Debug)]
pub enum MaybeNext<P> {
    Some(P),
    None,
    More,
}

#[inline]
pub(super) fn next<'a, I: DataPages, D: Decoder<'a>>(
    iter: &'a mut I,
    items: &mut VecDeque<D::DecodedState>,
    chunk_size: usize,
    decoder: &D,
) -> MaybeNext<Result<D::DecodedState, ArrowError>> {
    // front[a1, a2, a3, ...]back
    if items.len() > 1 {
        let item = items.pop_front().unwrap();
        return MaybeNext::Some(Ok(item));
    }
    match iter.next() {
        Err(e) => MaybeNext::Some(Err(e.into())),
        Ok(Some(page)) => {
            // there is a new page => consume the page from the start
            let maybe_page = decoder.build_state(page);
            let page = match maybe_page {
                Ok(page) => page,
                Err(e) => return MaybeNext::Some(Err(e)),
            };

            extend_from_new_page(page, chunk_size, items, decoder);

            if (items.len() == 1) && items.front().unwrap().len() < chunk_size {
                MaybeNext::More
            } else {
                let decoded = items.pop_front().unwrap();
                MaybeNext::Some(Ok(decoded))
            }
        }
        Ok(None) => {
            if let Some(decoded) = items.pop_front() {
                // we have a populated item and no more pages
                // the only case where an item's length may be smaller than chunk_size
                debug_assert!(decoded.len() <= chunk_size);
                MaybeNext::Some(Ok(decoded))
            } else {
                MaybeNext::None
            }
        }
    }
}

#[inline]
pub(super) fn dict_indices_decoder(page: &DataPage) -> hybrid_rle::HybridRleDecoder {
    let (_, _, indices_buffer) = split_buffer(page);

    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    hybrid_rle::HybridRleDecoder::new(indices_buffer, bit_width as u32, page.num_values())
}
