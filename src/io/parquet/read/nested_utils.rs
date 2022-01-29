use std::{collections::VecDeque, sync::Arc};

use parquet2::{
    encoding::hybrid_rle::HybridRleDecoder, page::DataPage, read::levels::get_bit_width,
};

use crate::{
    array::{Array, ListArray},
    bitmap::{Bitmap, MutableBitmap},
    buffer::Buffer,
    datatypes::{DataType, Field},
    error::{ArrowError, Result},
};

use super::{
    utils::{split_buffer, Decoder, MaybeNext, Pushable},
    DataPages,
};

/// trait describing deserialized repetition and definition levels
pub trait Nested: std::fmt::Debug {
    fn inner(&mut self) -> (Buffer<i64>, Option<Bitmap>);

    fn last_offset(&self) -> i64;

    fn push(&mut self, length: i64, is_valid: bool);

    fn offsets(&mut self) -> &[i64];

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

    #[inline]
    fn last_offset(&self) -> i64 {
        0
    }

    fn is_nullable(&self) -> bool {
        self.is_nullable
    }

    fn push(&mut self, _value: i64, _is_valid: bool) {
        self.length += 1
    }

    fn offsets(&mut self) -> &[i64] {
        &[]
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

    #[inline]
    fn last_offset(&self) -> i64 {
        *self.offsets.last().unwrap()
    }

    fn is_nullable(&self) -> bool {
        true
    }

    fn push(&mut self, value: i64, is_valid: bool) {
        self.offsets.push(value);
        self.validity.push(is_valid);
    }

    fn offsets(&mut self) -> &[i64] {
        &self.offsets
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

    #[inline]
    fn last_offset(&self) -> i64 {
        *self.offsets.last().unwrap()
    }

    fn push(&mut self, value: i64, _is_valid: bool) {
        self.offsets.push(value);
    }

    fn offsets(&mut self) -> &[i64] {
        &self.offsets
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

fn init_nested_recursive(field: &Field, capacity: usize, container: &mut Vec<Box<dyn Nested>>) {
    let is_nullable = field.is_nullable;

    use crate::datatypes::PhysicalType::*;
    match field.data_type().to_physical_type() {
        Null | Boolean | Primitive(_) | FixedSizeBinary | Binary | LargeBinary | Utf8
        | LargeUtf8 | Dictionary(_) => {
            container.push(Box::new(NestedPrimitive::new(is_nullable)) as Box<dyn Nested>)
        }
        List | LargeList | FixedSizeList => {
            if is_nullable {
                container.push(Box::new(NestedOptional::with_capacity(capacity)) as Box<dyn Nested>)
            } else {
                container.push(Box::new(NestedValid::with_capacity(capacity)) as Box<dyn Nested>)
            }
            match field.data_type().to_logical_type() {
                DataType::List(ref inner)
                | DataType::LargeList(ref inner)
                | DataType::FixedSizeList(ref inner, _) => {
                    init_nested_recursive(inner.as_ref(), capacity, container)
                }
                _ => unreachable!(),
            };
        }
        Struct => {
            container.push(Box::new(NestedPrimitive::new(is_nullable)) as Box<dyn Nested>);
            if let DataType::Struct(fields) = field.data_type().to_logical_type() {
                fields
                    .iter()
                    .for_each(|field| init_nested_recursive(field, capacity, container));
            } else {
                unreachable!()
            }
        }
        _ => todo!(),
    }
}

fn init_nested(field: &Field, capacity: usize) -> NestedState {
    let mut container = vec![];
    init_nested_recursive(field, capacity, &mut container);
    NestedState::new(container)
}

pub fn create_list(
    data_type: DataType,
    nested: &mut NestedState,
    values: Arc<dyn Array>,
) -> Result<Arc<dyn Array>> {
    Ok(match data_type {
        DataType::List(_) => {
            let (offsets, validity) = nested.nested.pop().unwrap().inner();

            let offsets = Buffer::<i32>::from_trusted_len_iter(offsets.iter().map(|x| *x as i32));
            Arc::new(ListArray::<i32>::from_data(
                data_type, offsets, values, validity,
            ))
        }
        DataType::LargeList(_) => {
            let (offsets, validity) = nested.nested.pop().unwrap().inner();

            Arc::new(ListArray::<i64>::from_data(
                data_type, offsets, values, validity,
            ))
        }
        _ => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Read nested datatype {:?}",
                data_type
            )))
        }
    })
}

pub struct NestedPage<'a> {
    repetitions: HybridRleDecoder<'a>,
    max_rep_level: u32,
    definitions: HybridRleDecoder<'a>,
    max_def_level: u32,
}

impl<'a> NestedPage<'a> {
    pub fn new(page: &'a DataPage) -> Self {
        let (rep_levels, def_levels, _, _) = split_buffer(page, page.descriptor());

        let max_rep_level = page.descriptor().max_rep_level();
        let max_def_level = page.descriptor().max_def_level();

        Self {
            repetitions: HybridRleDecoder::new(
                rep_levels,
                get_bit_width(max_rep_level),
                page.num_values(),
            ),
            max_rep_level: max_rep_level as u32,
            definitions: HybridRleDecoder::new(
                def_levels,
                get_bit_width(max_def_level),
                page.num_values(),
            ),
            max_def_level: max_def_level as u32,
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
        self.nested[0].num_values()
    }

    pub fn depth(&self) -> usize {
        // outermost is the number of rows
        self.nested.len()
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
    T::extend_from_state(&mut page, &mut values, &mut validity, remaining);

    // the number of values required is always fulfilled because
    // dremel assigns one (rep, def) to each value and we request
    // items that complete a row
    assert_eq!(values.len(), remaining);

    for nest in nested {
        let num_values = nest.num_values();
        let mut values = decoder.with_capacity(num_values);
        let mut validity = MutableBitmap::with_capacity(num_values);
        T::extend_from_state(&mut page, &mut values, &mut validity, num_values);
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
    field: &Field,
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
        init_nested(field, chunk_size)
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
        let mut nested = init_nested(field, chunk_size);
        extend_offsets2(page, &mut nested, chunk_size);
        items.push_back(nested)
    }

    // and return
    Ok(Some(nested))
}

fn extend_offsets2<'a>(page: &mut NestedPage<'a>, nested: &mut NestedState, additional: usize) {
    let mut values_count = vec![0; nested.depth()];
    let mut prev_def: u32 = 0;
    let mut is_first = true;

    let mut def_threshold = page.max_def_level;
    let thres = nested
        .nested
        .iter()
        .rev()
        .map(|nested| {
            let is_nullable = nested.is_nullable();
            def_threshold -= is_nullable as u32;
            def_threshold
        })
        .collect::<Vec<_>>();

    let max_rep = page.max_rep_level;

    let mut iter = page.repetitions.by_ref().zip(page.definitions.by_ref());

    let mut rows = 0;
    while rows < additional {
        // unwrap is ok because by definition there has to be a closing statement
        let (rep, def) = iter.next().unwrap();
        if rep == 0 {
            rows += 1
        }

        let mut closures = max_rep - rep;
        if prev_def <= 1 {
            closures = 1;
        };
        if is_first {
            // close on first run to ensure offsets start with 0.
            closures = max_rep;
            is_first = false;
        }

        nested
            .nested
            .iter_mut()
            .zip(values_count.iter())
            .enumerate()
            .skip(rep as usize)
            .take((rep + closures) as usize)
            .for_each(|(depth, (nested, length))| {
                let is_null = (def - rep) as usize == depth && depth == rep as usize;
                nested.push(*length, !is_null);
            });

        values_count
            .iter_mut()
            .zip(thres.iter())
            .enumerate()
            .for_each(|(depth, (values, thre))| {
                if depth == 1 {
                    if def >= *thre {
                        *values += 1
                    }
                } else if depth == 0 && def >= *thre {
                    *values += 1;
                }
            });
        prev_def = def;
    }

    // close validities
    nested
        .nested
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
        let (_, def_levels, _, _) = split_buffer(page, page.descriptor());

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
    field: &Field,
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
        //let array = BooleanArray::from_data(DataType::Boolean, values.into(), validity.into());
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
                extend_offsets1(&mut nested_page, state, field, nested_items, chunk_size);
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
