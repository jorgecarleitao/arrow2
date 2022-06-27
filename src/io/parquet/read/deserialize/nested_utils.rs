use std::collections::VecDeque;

use parquet2::{
    encoding::hybrid_rle::HybridRleDecoder,
    page::{split_buffer, DataPage},
    read::levels::get_bit_width,
};

use crate::{array::Array, bitmap::MutableBitmap, error::Result};

use super::super::DataPages;
pub use super::utils::Zip;
use super::utils::{DecodedState, Decoder, MaybeNext, Pushable};

/// trait describing deserialized repetition and definition levels
pub trait Nested: std::fmt::Debug + Send + Sync {
    fn inner(&mut self) -> (Vec<i64>, Option<MutableBitmap>);

    fn push(&mut self, length: i64, is_valid: bool);

    fn is_nullable(&self) -> bool;

    fn is_repeated(&self) -> bool {
        false
    }

    /// number of rows
    fn len(&self) -> usize;

    /// number of values associated to the primitive type this nested tracks
    fn num_values(&self) -> usize;
}

#[derive(Debug, Default)]
pub struct NestedPrimitive {
    is_nullable: bool,
    length: usize,
}

impl NestedPrimitive {
    pub fn new(is_nullable: bool) -> Self {
        Self {
            is_nullable,
            length: 0,
        }
    }
}

impl Nested for NestedPrimitive {
    fn inner(&mut self) -> (Vec<i64>, Option<MutableBitmap>) {
        (Default::default(), Default::default())
    }

    fn is_nullable(&self) -> bool {
        self.is_nullable
    }

    fn push(&mut self, _value: i64, _is_valid: bool) {
        self.length += 1
    }

    fn len(&self) -> usize {
        self.length
    }

    fn num_values(&self) -> usize {
        self.length
    }
}

#[derive(Debug, Default)]
pub struct NestedOptional {
    pub validity: MutableBitmap,
    pub offsets: Vec<i64>,
}

impl Nested for NestedOptional {
    fn inner(&mut self) -> (Vec<i64>, Option<MutableBitmap>) {
        let offsets = std::mem::take(&mut self.offsets);
        let validity = std::mem::take(&mut self.validity);
        (offsets, Some(validity))
    }

    fn is_nullable(&self) -> bool {
        true
    }

    fn is_repeated(&self) -> bool {
        true
    }

    fn push(&mut self, value: i64, is_valid: bool) {
        self.offsets.push(value);
        self.validity.push(is_valid);
    }

    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn num_values(&self) -> usize {
        self.offsets.last().copied().unwrap_or(0) as usize
    }
}

impl NestedOptional {
    pub fn with_capacity(capacity: usize) -> Self {
        let offsets = Vec::<i64>::with_capacity(capacity + 1);
        let validity = MutableBitmap::with_capacity(capacity);
        Self { validity, offsets }
    }
}

#[derive(Debug, Default)]
pub struct NestedValid {
    pub offsets: Vec<i64>,
}

impl Nested for NestedValid {
    fn inner(&mut self) -> (Vec<i64>, Option<MutableBitmap>) {
        let offsets = std::mem::take(&mut self.offsets);
        (offsets, None)
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn is_repeated(&self) -> bool {
        true
    }

    fn push(&mut self, value: i64, _is_valid: bool) {
        self.offsets.push(value);
    }

    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn num_values(&self) -> usize {
        self.offsets.last().copied().unwrap_or(0) as usize
    }
}

impl NestedValid {
    pub fn with_capacity(capacity: usize) -> Self {
        let offsets = Vec::<i64>::with_capacity(capacity + 1);
        Self { offsets }
    }
}

#[derive(Debug, Default)]
pub struct NestedStructValid {
    length: usize,
}

impl NestedStructValid {
    pub fn new() -> Self {
        Self { length: 0 }
    }
}

impl Nested for NestedStructValid {
    fn inner(&mut self) -> (Vec<i64>, Option<MutableBitmap>) {
        (Default::default(), None)
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn push(&mut self, _value: i64, _is_valid: bool) {
        self.length += 1;
    }

    fn len(&self) -> usize {
        self.length
    }

    fn num_values(&self) -> usize {
        self.length
    }
}

#[derive(Debug, Default)]
pub struct NestedStruct {
    validity: MutableBitmap,
}

impl NestedStruct {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            validity: MutableBitmap::with_capacity(capacity),
        }
    }
}

impl Nested for NestedStruct {
    fn inner(&mut self) -> (Vec<i64>, Option<MutableBitmap>) {
        (Default::default(), Some(std::mem::take(&mut self.validity)))
    }

    fn is_nullable(&self) -> bool {
        true
    }

    fn push(&mut self, _value: i64, is_valid: bool) {
        self.validity.push(is_valid)
    }

    fn len(&self) -> usize {
        self.validity.len()
    }

    fn num_values(&self) -> usize {
        self.validity.len()
    }
}

pub(super) fn read_optional_values<D, C, P>(items: D, values: &mut P, validity: &mut MutableBitmap)
where
    D: Iterator<Item = Option<C>>,
    C: Default,
    P: Pushable<C>,
{
    for item in items {
        if let Some(item) = item {
            values.push(item);
            validity.push(true);
        } else {
            values.push_null();
            validity.push(false);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InitNested {
    Primitive(bool),
    List(bool),
    Struct(bool),
}

fn init_nested(init: &[InitNested], capacity: usize) -> NestedState {
    let container = init
        .iter()
        .map(|init| match init {
            InitNested::Primitive(is_nullable) => {
                Box::new(NestedPrimitive::new(*is_nullable)) as Box<dyn Nested>
            }
            InitNested::List(is_nullable) => {
                if *is_nullable {
                    Box::new(NestedOptional::with_capacity(capacity)) as Box<dyn Nested>
                } else {
                    Box::new(NestedValid::with_capacity(capacity)) as Box<dyn Nested>
                }
            }
            InitNested::Struct(is_nullable) => {
                if *is_nullable {
                    Box::new(NestedStruct::with_capacity(capacity)) as Box<dyn Nested>
                } else {
                    Box::new(NestedStructValid::new()) as Box<dyn Nested>
                }
            }
        })
        .collect();
    NestedState::new(container)
}

pub struct NestedPage<'a> {
    iter: std::iter::Peekable<std::iter::Zip<HybridRleDecoder<'a>, HybridRleDecoder<'a>>>,
}

impl<'a> NestedPage<'a> {
    pub fn try_new(page: &'a DataPage) -> Result<Self> {
        let (rep_levels, def_levels, _) = split_buffer(page)?;

        let max_rep_level = page.descriptor.max_rep_level;
        let max_def_level = page.descriptor.max_def_level;

        let reps =
            HybridRleDecoder::new(rep_levels, get_bit_width(max_rep_level), page.num_values());
        let defs =
            HybridRleDecoder::new(def_levels, get_bit_width(max_def_level), page.num_values());

        let iter = reps.zip(defs).peekable();

        Ok(Self { iter })
    }

    // number of values (!= number of rows)
    pub fn len(&self) -> usize {
        self.iter.size_hint().0
    }
}

#[derive(Debug)]
pub struct NestedState {
    pub nested: Vec<Box<dyn Nested>>,
}

impl NestedState {
    pub fn new(nested: Vec<Box<dyn Nested>>) -> Self {
        Self { nested }
    }

    /// The number of rows in this state
    pub fn len(&self) -> usize {
        // outermost is the number of rows
        self.nested[0].len()
    }

    /// The number of values associated with the primitive type
    pub fn num_values(&self) -> usize {
        self.nested.last().unwrap().num_values()
    }
}

pub(super) fn extend_from_new_page<'a, T: Decoder<'a>>(
    mut page: T::State,
    items: &mut VecDeque<T::DecodedState>,
    nested: &VecDeque<NestedState>,
    decoder: &T,
) {
    let needed = nested.back().unwrap().num_values();

    let mut decoded = if let Some(decoded) = items.pop_back() {
        // there is a already a state => it must be incomplete...
        debug_assert!(
            decoded.len() < needed,
            "the temp page is expected to be incomplete ({} < {})",
            decoded.len(),
            needed
        );
        decoded
    } else {
        // there is no state => initialize it
        decoder.with_capacity(needed)
    };

    let remaining = needed - decoded.len();

    // extend the current state
    decoder.extend_from_state(&mut page, &mut decoded, remaining);

    // the number of values required is always fulfilled because
    // dremel assigns one (rep, def) to each value and we request
    // items that complete a row
    assert_eq!(decoded.len(), needed);

    items.push_back(decoded);

    for nest in nested.iter().skip(1) {
        let num_values = nest.num_values();
        let mut decoded = decoder.with_capacity(num_values);
        decoder.extend_from_state(&mut page, &mut decoded, num_values);
        items.push_back(decoded);
    }
}

/// Extends `state` by consuming `page`, optionally extending `items` if `page`
/// has less items than `chunk_size`
pub fn extend_offsets1<'a>(
    page: &mut NestedPage<'a>,
    init: &[InitNested],
    items: &mut VecDeque<NestedState>,
    chunk_size: Option<usize>,
) {
    let capacity = chunk_size.unwrap_or(0);
    let chunk_size = chunk_size.unwrap_or(usize::MAX);

    let mut nested = if let Some(nested) = items.pop_back() {
        // there is a already a state => it must be incomplete...
        debug_assert!(
            nested.len() < chunk_size,
            "the temp array is expected to be incomplete"
        );
        nested
    } else {
        // there is no state => initialize it
        init_nested(init, capacity)
    };

    let remaining = chunk_size - nested.len();

    // extend the current state
    extend_offsets2(page, &mut nested, remaining);
    items.push_back(nested);

    while page.len() > 0 {
        let mut nested = init_nested(init, capacity);
        extend_offsets2(page, &mut nested, chunk_size);
        items.push_back(nested);
    }
}

fn extend_offsets2<'a>(page: &mut NestedPage<'a>, nested: &mut NestedState, additional: usize) {
    let nested = &mut nested.nested;
    let mut values_count = vec![0; nested.len()];

    for (depth, nest) in nested.iter().enumerate().skip(1) {
        values_count[depth - 1] = nest.len() as i64
    }
    values_count[nested.len() - 1] = nested[nested.len() - 1].len() as i64;

    let mut cum_sum = vec![0u32; nested.len() + 1];
    for (i, nest) in nested.iter().enumerate() {
        let delta = nest.is_nullable() as u32 + nest.is_repeated() as u32;
        cum_sum[i + 1] = cum_sum[i] + delta;
    }

    let mut rows = 0;
    while let Some((rep, def)) = page.iter.next() {
        if rep == 0 {
            rows += 1;
        }

        for (depth, (nest, length)) in nested.iter_mut().zip(values_count.iter()).enumerate() {
            if depth as u32 >= rep && def >= cum_sum[depth] {
                let is_valid = nest.is_nullable() && def != cum_sum[depth];
                nest.push(*length, is_valid)
            }
        }

        for (depth, nest) in nested.iter().enumerate().skip(1) {
            values_count[depth - 1] = nest.len() as i64
        }
        values_count[nested.len() - 1] = nested[nested.len() - 1].len() as i64;

        let next_rep = page.iter.peek().map(|x| x.0).unwrap_or(0);

        if next_rep == 0 && rows == additional.saturating_add(1) {
            break;
        }
    }
}

#[derive(Debug)]
pub struct Optional<'a> {
    iter: HybridRleDecoder<'a>,
    max_def: u32,
}

impl<'a> Iterator for Optional<'a> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().and_then(|def| {
            if def == self.max_def {
                Some(true)
            } else if def == self.max_def - 1 {
                Some(false)
            } else {
                self.next()
            }
        })
    }
}

impl<'a> Optional<'a> {
    pub fn try_new(page: &'a DataPage) -> Result<Self> {
        let (_, def_levels, _) = split_buffer(page)?;

        let max_def = page.descriptor.max_def_level;

        Ok(Self {
            iter: HybridRleDecoder::new(def_levels, get_bit_width(max_def), page.num_values()),
            max_def: max_def as u32,
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        unreachable!();
    }
}

#[inline]
pub(super) fn next<'a, I, D>(
    iter: &'a mut I,
    items: &mut VecDeque<D::DecodedState>,
    nested_items: &mut VecDeque<NestedState>,
    init: &[InitNested],
    chunk_size: Option<usize>,
    decoder: &D,
) -> MaybeNext<Result<(NestedState, D::DecodedState)>>
where
    I: DataPages,
    D: Decoder<'a>,
{
    // front[a1, a2, a3, ...]back
    if items.len() > 1 {
        let nested = nested_items.pop_front().unwrap();
        let decoded = items.pop_front().unwrap();
        return MaybeNext::Some(Ok((nested, decoded)));
    }
    match iter.next() {
        Err(e) => MaybeNext::Some(Err(e.into())),
        Ok(None) => {
            if let Some(nested) = nested_items.pop_front() {
                // we have a populated item and no more pages
                // the only case where an item's length may be smaller than chunk_size
                let decoded = items.pop_front().unwrap();
                MaybeNext::Some(Ok((nested, decoded)))
            } else {
                MaybeNext::None
            }
        }
        Ok(Some(page)) => {
            // there is a new page => consume the page from the start
            let nested_page = NestedPage::try_new(page);
            let mut nested_page = match nested_page {
                Ok(page) => page,
                Err(e) => return MaybeNext::Some(Err(e)),
            };

            extend_offsets1(&mut nested_page, init, nested_items, chunk_size);

            let maybe_page = decoder.build_state(page);
            let page = match maybe_page {
                Ok(page) => page,
                Err(e) => return MaybeNext::Some(Err(e)),
            };

            extend_from_new_page(page, items, nested_items, decoder);

            if nested_items.front().unwrap().len() < chunk_size.unwrap_or(0) {
                MaybeNext::More
            } else {
                let nested = nested_items.pop_front().unwrap();
                let decoded = items.pop_front().unwrap();
                MaybeNext::Some(Ok((nested, decoded)))
            }
        }
    }
}

pub type NestedArrayIter<'a> =
    Box<dyn Iterator<Item = Result<(NestedState, Box<dyn Array>)>> + Send + Sync + 'a>;
