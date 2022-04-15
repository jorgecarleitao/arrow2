use std::collections::VecDeque;

use parquet2::{
    deserialize::SliceFilteredIter,
    encoding::{hybrid_rle, Encoding},
    page::{DataPage, PrimitivePageDict},
    schema::Repetition,
    types::decode,
    types::NativeType as ParquetNativeType,
};

use crate::{
    array::MutablePrimitiveArray, bitmap::MutableBitmap, datatypes::DataType, error::Result,
    types::NativeType,
};

use super::super::utils;
use super::super::utils::{get_selected_rows, FilteredOptionalPageValidity, OptionalPageValidity};
use super::super::DataPages;

#[derive(Debug)]
struct FilteredRequiredValues<'a> {
    values: SliceFilteredIter<std::slice::ChunksExact<'a, u8>>,
}

impl<'a> FilteredRequiredValues<'a> {
    pub fn new<P: ParquetNativeType>(page: &'a DataPage) -> Self {
        let (_, _, values) = utils::split_buffer(page);
        assert_eq!(values.len() % std::mem::size_of::<P>(), 0);

        let values = values.chunks_exact(std::mem::size_of::<P>());

        let rows = get_selected_rows(page);
        let values = SliceFilteredIter::new(values, rows);

        Self { values }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.size_hint().0
    }
}

#[derive(Debug)]
pub(super) struct Values<'a> {
    pub values: std::slice::ChunksExact<'a, u8>,
}

impl<'a> Values<'a> {
    pub fn new<P: ParquetNativeType>(page: &'a DataPage) -> Self {
        let (_, _, values) = utils::split_buffer(page);
        assert_eq!(values.len() % std::mem::size_of::<P>(), 0);
        Self {
            values: values.chunks_exact(std::mem::size_of::<P>()),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.size_hint().0
    }
}

#[derive(Debug)]
pub(super) struct ValuesDictionary<'a, P>
where
    P: ParquetNativeType,
{
    pub values: hybrid_rle::HybridRleDecoder<'a>,
    pub dict: &'a [P],
}

impl<'a, P> ValuesDictionary<'a, P>
where
    P: ParquetNativeType,
{
    pub fn new(page: &'a DataPage, dict: &'a PrimitivePageDict<P>) -> Self {
        let values = utils::dict_indices_decoder(page);

        Self {
            dict: dict.values(),
            values,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.size_hint().0
    }
}

// The state of a `DataPage` of `Primitive` parquet primitive type
#[derive(Debug)]
enum State<'a, P>
where
    P: ParquetNativeType,
{
    Optional(OptionalPageValidity<'a>, Values<'a>),
    Required(Values<'a>),
    RequiredDictionary(ValuesDictionary<'a, P>),
    OptionalDictionary(OptionalPageValidity<'a>, ValuesDictionary<'a, P>),
    FilteredRequired(FilteredRequiredValues<'a>),
    FilteredOptional(FilteredOptionalPageValidity<'a>, Values<'a>),
}

impl<'a, P> utils::PageState<'a> for State<'a, P>
where
    P: ParquetNativeType,
{
    fn len(&self) -> usize {
        match self {
            State::Optional(optional, _) => optional.len(),
            State::Required(values) => values.len(),
            State::RequiredDictionary(values) => values.len(),
            State::OptionalDictionary(optional, _) => optional.len(),
            State::FilteredRequired(values) => values.len(),
            State::FilteredOptional(optional, _) => optional.len(),
        }
    }
}

#[derive(Debug)]
struct PrimitiveDecoder<T, P, F>
where
    T: NativeType,
    P: ParquetNativeType,
    F: Fn(P) -> T,
{
    phantom: std::marker::PhantomData<T>,
    phantom_p: std::marker::PhantomData<P>,
    op: F,
}

impl<'a, T, P, F> PrimitiveDecoder<T, P, F>
where
    T: NativeType,
    P: ParquetNativeType,
    F: Fn(P) -> T,
{
    #[inline]
    fn new(op: F) -> Self {
        Self {
            phantom: std::marker::PhantomData,
            phantom_p: std::marker::PhantomData,
            op,
        }
    }
}

impl<'a, T> utils::DecodedState<'a> for (Vec<T>, MutableBitmap) {
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<'a, T, P, F> utils::Decoder<'a> for PrimitiveDecoder<T, P, F>
where
    T: NativeType,
    P: ParquetNativeType,
    F: Copy + Fn(P) -> T,
{
    type State = State<'a, P>;
    type DecodedState = (Vec<T>, MutableBitmap);

    fn build_state(&self, page: &'a DataPage) -> Result<Self::State> {
        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;
        let is_filtered = page.selected_rows().is_some();

        match (
            page.encoding(),
            page.dictionary_page(),
            is_optional,
            is_filtered,
        ) {
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false, false) => {
                let dict = dict.as_any().downcast_ref().unwrap();
                Ok(State::RequiredDictionary(ValuesDictionary::new(page, dict)))
            }
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true, false) => {
                let dict = dict.as_any().downcast_ref().unwrap();

                Ok(State::OptionalDictionary(
                    OptionalPageValidity::new(page),
                    ValuesDictionary::new(page, dict),
                ))
            }
            (Encoding::Plain, _, true, false) => {
                let validity = OptionalPageValidity::new(page);
                let values = Values::new::<P>(page);

                Ok(State::Optional(validity, values))
            }
            (Encoding::Plain, _, false, false) => Ok(State::Required(Values::new::<P>(page))),
            (Encoding::Plain, _, false, true) => Ok(State::FilteredRequired(
                FilteredRequiredValues::new::<P>(page),
            )),
            (Encoding::Plain, _, true, true) => Ok(State::FilteredOptional(
                FilteredOptionalPageValidity::new(page),
                Values::new::<P>(page),
            )),
            _ => Err(utils::not_implemented(page)),
        }
    }

    fn with_capacity(&self, capacity: usize) -> Self::DecodedState {
        (
            Vec::<T>::with_capacity(capacity),
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
            State::Optional(page_validity, page_values) => utils::extend_from_decoder(
                validity,
                page_validity,
                Some(remaining),
                values,
                page_values.values.by_ref().map(decode).map(self.op),
            ),
            State::Required(page) => {
                values.extend(
                    page.values
                        .by_ref()
                        .map(decode)
                        .map(self.op)
                        .take(remaining),
                );
            }
            State::OptionalDictionary(page_validity, page_values) => {
                let op1 = |index: u32| page_values.dict[index as usize];
                utils::extend_from_decoder(
                    validity,
                    page_validity,
                    Some(remaining),
                    values,
                    &mut page_values.values.by_ref().map(op1).map(self.op),
                )
            }
            State::RequiredDictionary(page) => {
                let op1 = |index: u32| page.dict[index as usize];
                values.extend(page.values.by_ref().map(op1).map(self.op).take(remaining));
            }
            State::FilteredRequired(page) => {
                values.extend(
                    page.values
                        .by_ref()
                        .map(decode)
                        .map(self.op)
                        .take(remaining),
                );
            }
            State::FilteredOptional(page_validity, page_values) => {
                utils::extend_from_decoder(
                    validity,
                    page_validity,
                    Some(remaining),
                    values,
                    page_values.values.by_ref().map(decode).map(self.op),
                );
            }
        }
    }
}

pub(super) fn finish<T: NativeType>(
    data_type: &DataType,
    values: Vec<T>,
    validity: MutableBitmap,
) -> MutablePrimitiveArray<T> {
    let validity = if validity.is_empty() {
        None
    } else {
        Some(validity)
    };
    MutablePrimitiveArray::from_data(data_type.clone(), values, validity)
}

/// An iterator adapter over [`DataPages`] assumed to be encoded as primitive arrays
#[derive(Debug)]
pub struct Iter<T, I, P, F>
where
    I: DataPages,
    T: NativeType,
    P: ParquetNativeType,
    F: Fn(P) -> T,
{
    iter: I,
    data_type: DataType,
    items: VecDeque<(Vec<T>, MutableBitmap)>,
    chunk_size: usize,
    op: F,
    phantom: std::marker::PhantomData<P>,
}

impl<T, I, P, F> Iter<T, I, P, F>
where
    I: DataPages,
    T: NativeType,

    P: ParquetNativeType,
    F: Copy + Fn(P) -> T,
{
    pub fn new(iter: I, data_type: DataType, chunk_size: usize, op: F) -> Self {
        Self {
            iter,
            data_type,
            items: VecDeque::new(),
            chunk_size,
            op,
            phantom: Default::default(),
        }
    }
}

impl<T, I, P, F> Iterator for Iter<T, I, P, F>
where
    I: DataPages,
    T: NativeType,
    P: ParquetNativeType,
    F: Copy + Fn(P) -> T,
{
    type Item = Result<MutablePrimitiveArray<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_state = utils::next(
            &mut self.iter,
            &mut self.items,
            self.chunk_size,
            &PrimitiveDecoder::new(self.op),
        );
        match maybe_state {
            utils::MaybeNext::Some(Ok((values, validity))) => {
                Some(Ok(finish(&self.data_type, values, validity)))
            }
            utils::MaybeNext::Some(Err(e)) => Some(Err(e)),
            utils::MaybeNext::None => None,
            utils::MaybeNext::More => self.next(),
        }
    }
}
