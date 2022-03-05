use std::{collections::VecDeque, sync::Arc};

use parquet2::{
    encoding::hybrid_rle::HybridRleDecoder, page::DataPage, read::levels::get_bit_width,
};

use crate::{
    array::Array,
    bitmap::{Bitmap, MutableBitmap},
    buffer::Buffer,
    error::Result,
};

use super::super::DataPages;
use super::utils::{split_buffer, Decoder, MaybeNext, Pushable};

/// trait describing deserialized repetition and definition levels
pub trait Nested: std::fmt::Debug + Send + Sync {
    fn inner(&mut self) -> (Buffer<i64>, Option<Bitmap>);

    fn push(&mut self, length: i64, is_valid: bool);

    fn close(&mut self, length: i64);

    fn is_nullable(&self) -> bool;

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
    fn inner(&mut self) -> (Buffer<i64>, Option<Bitmap>) {
        (Default::default(), Default::default())
    }

    fn is_nullable(&self) -> bool {
        self.is_nullable
    }

    fn push(&mut self, _value: i64, _is_valid: bool) {
        self.length += 1
    }

    fn close(&mut self, _length: i64) {}

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
    fn inner(&mut self) -> (Buffer<i64>, Option<Bitmap>) {
        let offsets = std::mem::take(&mut self.offsets);
        let validity = std::mem::take(&mut self.validity);
        (offsets.into(), validity.into())
    }

    fn is_nullable(&self) -> bool {
        true
    }

    fn push(&mut self, value: i64, is_valid: bool) {
        self.offsets.push(value);
        self.validity.push(is_valid);
    }

    fn close(&mut self, length: i64) {
        self.offsets.push(length)
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
    fn inner(&mut self) -> (Buffer<i64>, Option<Bitmap>) {
        let offsets = std::mem::take(&mut self.offsets);
        (offsets.into(), None)
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn push(&mut self, value: i64, _is_valid: bool) {
        self.offsets.push(value);
    }

    fn close(&mut self, length: i64) {
        self.offsets.push(length)
    }

    fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
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
    fn inner(&mut self) -> (Buffer<i64>, Option<Bitmap>) {
        (Default::default(), None)
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn push(&mut self, _value: i64, _is_valid: bool) {
        self.length += 1;
    }

    fn close(&mut self, _length: i64) {}

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
    fn inner(&mut self) -> (Buffer<i64>, Option<Bitmap>) {
        (Default::default(), None)
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn push(&mut self, _value: i64, is_valid: bool) {
        self.validity.push(is_valid)
    }

    fn close(&mut self, _length: i64) {}

    fn len(&self) -> usize {
        self.validity.len()
    }

    fn num_values(&self) -> usize {
        self.validity.len()
    }
}

pub(super) fn read_optional_values<D, C, G, P>(
    def_levels: D,
    max_def: u32,
    mut new_values: G,
    values: &mut P,
    validity: &mut MutableBitmap,
    mut remaining: usize,
) where
    D: Iterator<Item = u32>,
    G: Iterator<Item = C>,
    C: Default,
    P: Pushable<C>,
{
    for def in def_levels {
        if def == max_def {
            values.push(new_values.next().unwrap());
            validity.push(true);
            remaining -= 1;
        } else if def == max_def - 1 {
            values.push(C::default());
            validity.push(false);
            remaining -= 1;
        }
        if remaining == 0 {
            break;
        }
    }
}

#[derive(Debug, Clone)]
pub enum InitNested {
    Primitive(bool),
    List(Box<InitNested>, bool),
    Struct(Box<InitNested>, bool),
}

impl InitNested {
    pub fn is_primitive(&self) -> bool {
        matches!(self, Self::Primitive(_))
    }
}

fn init_nested_recursive(init: &InitNested, capacity: usize, container: &mut Vec<Box<dyn Nested>>) {
    match init {
        InitNested::Primitive(is_nullable) => {
            container.push(Box::new(NestedPrimitive::new(*is_nullable)) as Box<dyn Nested>)
        }
        InitNested::List(inner, is_nullable) => {
            container.push(if *is_nullable {
                Box::new(NestedOptional::with_capacity(capacity)) as Box<dyn Nested>
            } else {
                Box::new(NestedValid::with_capacity(capacity)) as Box<dyn Nested>
            });
            init_nested_recursive(inner, capacity, container)
        }
        InitNested::Struct(inner, is_nullable) => {
            if *is_nullable {
                container.push(Box::new(NestedStruct::with_capacity(capacity)) as Box<dyn Nested>)
            } else {
                container.push(Box::new(NestedStructValid::new()) as Box<dyn Nested>)
            }
            init_nested_recursive(inner, capacity, container)
        }
    }
}

fn init_nested(init: &InitNested, capacity: usize) -> NestedState {
    let mut container = vec![];
    init_nested_recursive(init, capacity, &mut container);
    NestedState::new(container)
}

pub struct NestedPage<'a> {
    repetitions: HybridRleDecoder<'a>,
    _max_rep_level: u32,
    definitions: HybridRleDecoder<'a>,
}

impl<'a> NestedPage<'a> {
    pub fn new(page: &'a DataPage) -> Self {
        let (rep_levels, def_levels, _) = split_buffer(page);

        let max_rep_level = page.descriptor().max_rep_level();
        let max_def_level = page.descriptor().max_def_level();

        Self {
            repetitions: HybridRleDecoder::new(
                rep_levels,
                get_bit_width(max_rep_level),
                page.num_values(),
            ),
            _max_rep_level: max_rep_level as u32,
            definitions: HybridRleDecoder::new(
                def_levels,
                get_bit_width(max_def_level),
                page.num_values(),
            ),
        }
    }

    // number of values (!= number of rows)
    pub fn len(&self) -> usize {
        self.repetitions.size_hint().0
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

pub(super) fn extend_from_new_page<'a, T: Decoder<'a, C, P>, C: Default, P: Pushable<C>>(
    mut page: T::State,
    state: Option<(P, MutableBitmap)>,
    items: &mut VecDeque<(P, MutableBitmap)>,
    nested_state: &Option<NestedState>,
    nested: &VecDeque<NestedState>,
    decoder: &T,
) -> Result<(P, MutableBitmap)> {
    let needed = nested_state
        .as_ref()
        .map(|x| x.num_values())
        // unwrap is fine because either there is a state or the state is in nested
        .unwrap_or_else(|| nested.back().unwrap().num_values());

    let (mut values, mut validity) = if let Some((values, validity)) = state {
        // there is a already a state => it must be incomplete...
        debug_assert!(
            values.len() < needed,
            "the temp array is expected to be incomplete"
        );
        (values, validity)
    } else {
        // there is no state => initialize it
        (
            decoder.with_capacity(needed),
            MutableBitmap::with_capacity(needed),
        )
    };

    let remaining = needed - values.len();

    // extend the current state
    decoder.extend_from_state(&mut page, &mut values, &mut validity, remaining);

    // the number of values required is always fulfilled because
    // dremel assigns one (rep, def) to each value and we request
    // items that complete a row
    assert_eq!(values.len(), remaining);

    for nest in nested {
        let num_values = nest.num_values();
        let mut values = decoder.with_capacity(num_values);
        let mut validity = MutableBitmap::with_capacity(num_values);
        decoder.extend_from_state(&mut page, &mut values, &mut validity, num_values);
        items.push_back((values, validity));
    }

    assert_eq!(items.len(), nested.len());

    // and return this item
    Ok((values, validity))
}

/// Extends `state` by consuming `page`, optionally extending `items` if `page`
/// has less items than `chunk_size`
pub fn extend_offsets1<'a>(
    page: &mut NestedPage<'a>,
    state: Option<NestedState>,
    init: &InitNested,
    items: &mut VecDeque<NestedState>,
    chunk_size: usize,
) -> Result<Option<NestedState>> {
    let mut nested = if let Some(nested) = state {
        // there is a already a state => it must be incomplete...
        debug_assert!(
            nested.len() < chunk_size,
            "the temp array is expected to be incomplete"
        );
        nested
    } else {
        // there is no state => initialize it
        init_nested(init, chunk_size)
    };

    let remaining = chunk_size - nested.len();

    // extend the current state
    extend_offsets2(page, &mut nested, remaining);

    if nested.len() < chunk_size {
        // the whole page was consumed and we still do not have enough items
        // => push the values to `items` so that it can be continued later
        items.push_back(nested);
        // and indicate that there is no item available
        return Ok(None);
    }

    while page.len() > 0 {
        let mut nested = init_nested(init, chunk_size);
        extend_offsets2(page, &mut nested, chunk_size);
        items.push_back(nested)
    }

    // and return
    Ok(Some(nested))
}

fn extend_offsets2<'a>(page: &mut NestedPage<'a>, nested: &mut NestedState, additional: usize) {
    let nested = &mut nested.nested;
    let mut values_count = vec![0; nested.len()];

    let mut cum_sum = vec![0u32; nested.len() + 1];
    for (i, nest) in nested.iter().enumerate() {
        let delta = if nest.is_nullable() { 2 } else { 1 };
        cum_sum[i + 1] = cum_sum[i] + delta;
    }

    let mut iter = page.repetitions.by_ref().zip(page.definitions.by_ref());

    let mut rows = 0;
    while rows < additional {
        // unwrap is ok because by definition there has to be a closing statement
        let (rep, def) = iter.next().unwrap();
        if rep == 0 {
            rows += 1
        }

        for (depth, (nest, length)) in nested.iter_mut().zip(values_count.iter()).enumerate() {
            if depth as u32 >= rep && def >= cum_sum[depth] {
                let is_valid = nest.is_nullable() && def as u32 != cum_sum[depth];
                nest.push(*length, is_valid)
            }
        }

        for (depth, nest) in nested.iter().enumerate().skip(1) {
            values_count[depth - 1] = nest.len() as i64
        }
        values_count[nested.len() - 1] = nested[nested.len() - 1].len() as i64
    }

    // close validities
    nested
        .iter_mut()
        .zip(values_count.iter())
        .for_each(|(nested, length)| {
            nested.close(*length);
        });
}

// The state of an optional DataPage with a boolean physical type
#[derive(Debug)]
pub struct Optional<'a> {
    pub definition_levels: HybridRleDecoder<'a>,
    max_def: u32,
}

impl<'a> Optional<'a> {
    pub fn new(page: &'a DataPage) -> Self {
        let (_, def_levels, _) = split_buffer(page);

        let max_def = page.descriptor().max_def_level();

        Self {
            definition_levels: HybridRleDecoder::new(
                def_levels,
                get_bit_width(max_def),
                page.num_values(),
            ),
            max_def: max_def as u32,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.definition_levels.size_hint().0
    }

    #[inline]
    pub fn max_def(&self) -> u32 {
        self.max_def
    }
}

#[inline]
pub(super) fn next<'a, I, C, P, D>(
    iter: &'a mut I,
    items: &mut VecDeque<(P, MutableBitmap)>,
    nested_items: &mut VecDeque<NestedState>,
    init: &InitNested,
    chunk_size: usize,
    decoder: &D,
) -> MaybeNext<Result<(NestedState, P, MutableBitmap)>>
where
    I: DataPages,
    C: Default,
    P: Pushable<C>,
    D: Decoder<'a, C, P>,
{
    // back[a1, a2, a3, ...]front
    if items.len() > 1 {
        let nested = nested_items.pop_back().unwrap();
        let (values, validity) = items.pop_back().unwrap();
        return MaybeNext::Some(Ok((nested, values, validity)));
    }
    match (nested_items.pop_back(), items.pop_back(), iter.next()) {
        (_, _, Err(e)) => MaybeNext::Some(Err(e.into())),
        (None, None, Ok(None)) => MaybeNext::None,
        (state, p_state, Ok(Some(page))) => {
            // the invariant
            assert_eq!(state.is_some(), p_state.is_some());

            // there is a new page => consume the page from the start
            let mut nested_page = NestedPage::new(page);

            // read next chunk from `nested_page` and get number of values to read
            let maybe_nested =
                extend_offsets1(&mut nested_page, state, init, nested_items, chunk_size);
            let nested = match maybe_nested {
                Ok(nested) => nested,
                Err(e) => return MaybeNext::Some(Err(e)),
            };
            // at this point we know whether there were enough rows in `page`
            // to fill chunk_size or not (`nested.is_some()`)
            // irrespectively, we need to consume the values from the page

            let maybe_page = decoder.build_state(page);
            let page = match maybe_page {
                Ok(page) => page,
                Err(e) => return MaybeNext::Some(Err(e)),
            };

            let maybe_array = extend_from_new_page::<D, _, _>(
                page,
                p_state,
                items,
                &nested,
                nested_items,
                decoder,
            );
            let state = match maybe_array {
                Ok(s) => s,
                Err(e) => return MaybeNext::Some(Err(e)),
            };
            match nested {
                Some(p_state) => MaybeNext::Some(Ok((p_state, state.0, state.1))),
                None => MaybeNext::More,
            }
        }
        (Some(nested), Some((values, validity)), Ok(None)) => {
            // we have a populated item and no more pages
            // the only case where an item's length may be smaller than chunk_size
            MaybeNext::Some(Ok((nested, values, validity)))
        }
        (Some(_), None, _) => unreachable!(),
        (None, Some(_), _) => unreachable!(),
    }
}

pub type NestedArrayIter<'a> =
    Box<dyn Iterator<Item = Result<(NestedState, Arc<dyn Array>)>> + Send + Sync + 'a>;
