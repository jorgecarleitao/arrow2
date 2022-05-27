use std::collections::VecDeque;

use parquet2::{
    encoding::Encoding, page::DataPage, schema::Repetition, types::decode,
    types::NativeType as ParquetNativeType,
};

use crate::{
    array::PrimitiveArray, bitmap::MutableBitmap, datatypes::DataType, error::Result,
    types::NativeType,
};

use super::super::nested_utils::*;
use super::super::utils;
use super::super::DataPages;
use super::basic::{Values, ValuesDictionary};

// The state of a `DataPage` of `Primitive` parquet primitive type
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum State<'a, P>
where
    P: ParquetNativeType,
{
    Optional(Optional<'a>, Values<'a>),
    Required(Values<'a>),
    RequiredDictionary(ValuesDictionary<'a, P>),
    OptionalDictionary(Optional<'a>, ValuesDictionary<'a, P>),
}

impl<'a, P> utils::PageState<'a> for State<'a, P>
where
    P: ParquetNativeType,
{
    fn len(&self) -> usize {
        match self {
            State::Optional(optional, _) => optional.len(),
            State::Required(required) => required.len(),
            State::RequiredDictionary(required) => required.len(),
            State::OptionalDictionary(optional, _) => optional.len(),
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
                    Optional::new(page),
                    ValuesDictionary::new(page, dict),
                ))
            }
            (Encoding::Plain, _, true, false) => {
                Ok(State::Optional(Optional::new(page), Values::new::<P>(page)))
            }
            (Encoding::Plain, _, false, false) => Ok(State::Required(Values::new::<P>(page))),
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
        additional: usize,
    ) {
        let (values, validity) = decoded;
        match state {
            State::Optional(page_validity, page_values) => {
                let items = page_validity.by_ref().take(additional);
                let items = Zip::new(items, page_values.values.by_ref().map(decode).map(self.op));

                read_optional_values(items, values, validity)
            }
            State::Required(page) => {
                values.extend(
                    page.values
                        .by_ref()
                        .map(decode)
                        .map(self.op)
                        .take(additional),
                );
            }
            State::RequiredDictionary(page) => {
                let op1 = |index: u32| page.dict[index as usize];
                values.extend(page.values.by_ref().map(op1).map(self.op).take(additional));
            }
            State::OptionalDictionary(page_validity, page_values) => {
                let op1 = |index: u32| page_values.dict[index as usize];

                let items = page_validity.by_ref().take(additional);
                let items = Zip::new(items, page_values.values.by_ref().map(op1).map(self.op));

                read_optional_values(items, values, validity)
            }
        }
    }
}

fn finish<T: NativeType>(
    data_type: &DataType,
    values: Vec<T>,
    validity: MutableBitmap,
) -> PrimitiveArray<T> {
    PrimitiveArray::from_data(data_type.clone(), values.into(), validity.into())
}

/// An iterator adapter over [`DataPages`] assumed to be encoded as boolean arrays
#[derive(Debug)]
pub struct ArrayIterator<T, I, P, F>
where
    I: DataPages,
    T: NativeType,

    P: ParquetNativeType,
    F: Copy + Fn(P) -> T,
{
    iter: I,
    init: Vec<InitNested>,
    data_type: DataType,
    // invariant: items.len() == nested.len()
    items: VecDeque<(Vec<T>, MutableBitmap)>,
    nested: VecDeque<NestedState>,
    chunk_size: usize,
    decoder: PrimitiveDecoder<T, P, F>,
}

impl<T, I, P, F> ArrayIterator<T, I, P, F>
where
    I: DataPages,
    T: NativeType,

    P: ParquetNativeType,
    F: Copy + Fn(P) -> T,
{
    pub fn new(iter: I, init: Vec<InitNested>, data_type: DataType, chunk_size: usize, op: F) -> Self {
        Self {
            iter,
            init,
            data_type,
            items: VecDeque::new(),
            nested: VecDeque::new(),
            chunk_size,
            decoder: PrimitiveDecoder::new(op),
        }
    }
}

impl<T, I, P, F> Iterator for ArrayIterator<T, I, P, F>
where
    I: DataPages,
    T: NativeType,

    P: ParquetNativeType,
    F: Copy + Fn(P) -> T,
{
    type Item = Result<(NestedState, PrimitiveArray<T>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_state = next(
            &mut self.iter,
            &mut self.items,
            &mut self.nested,
            &self.init,
            self.chunk_size,
            &self.decoder,
        );
        match maybe_state {
            utils::MaybeNext::Some(Ok((nested, state))) => {
                Some(Ok((nested, finish(&self.data_type, state.0, state.1))))
            }
            utils::MaybeNext::Some(Err(e)) => Some(Err(e)),
            utils::MaybeNext::None => None,
            utils::MaybeNext::More => self.next(),
        }
    }
}
