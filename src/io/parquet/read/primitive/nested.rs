use std::collections::VecDeque;

use parquet2::{
    encoding::Encoding, page::DataPage, schema::Repetition, types::NativeType as ParquetNativeType,
};

use crate::{
    array::PrimitiveArray, bitmap::MutableBitmap, datatypes::DataType, error::Result,
    io::parquet::read::utils::MaybeNext, types::NativeType,
};

use super::super::nested_utils::*;
use super::super::utils;
use super::super::DataPages;
use super::basic::Values;

// The state of a `DataPage` of `Primitive` parquet primitive type
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum State<'a, P>
where
    P: ParquetNativeType,
{
    Optional(Optional<'a>, Values<'a, P>),
    Required(Values<'a, P>),
    //RequiredDictionary(ValuesDictionary<'a, T, P, F>),
    //OptionalDictionary(Optional<'a>, ValuesDictionary<'a, T, P, F>),
}

impl<'a, P> utils::PageState<'a> for State<'a, P>
where
    P: ParquetNativeType,
{
    fn len(&self) -> usize {
        match self {
            State::Optional(optional, _) => optional.len(),
            State::Required(required) => required.len(),
            //State::RequiredDictionary(required) => required.len(),
            //State::OptionalDictionary(optional, _) => optional.len(),
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

impl<'a, T, P, F> utils::Decoder<'a, T, Vec<T>> for PrimitiveDecoder<T, P, F>
where
    T: NativeType,
    P: ParquetNativeType,
    F: Copy + Fn(P) -> T,
{
    type State = State<'a, P>;

    fn build_state(&self, page: &'a DataPage) -> Result<Self::State> {
        let is_optional =
            page.descriptor().type_().get_basic_info().repetition() == &Repetition::Optional;

        match (page.encoding(), page.dictionary_page(), is_optional) {
            /*(Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false) => {
                todo!()
            }
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true) => {
                let dict = dict.as_any().downcast_ref().unwrap();
                Ok(State::OptionalDictionary(OptionalDictionaryPage::new(
                    page, dict, self.op2,
                )))
            }*/
            (Encoding::Plain, None, true) => {
                Ok(State::Optional(Optional::new(page), Values::new(page)))
            }
            (Encoding::Plain, None, false) => Ok(State::Required(Values::new(page))),
            _ => Err(utils::not_implemented(
                &page.encoding(),
                is_optional,
                false,
                "any",
                "Primitive",
            )),
        }
    }

    fn with_capacity(&self, capacity: usize) -> Vec<T> {
        Vec::<T>::with_capacity(capacity)
    }

    fn extend_from_state(
        &self,
        state: &mut Self::State,
        values: &mut Vec<T>,
        validity: &mut MutableBitmap,
        remaining: usize,
    ) {
        match state {
            State::Optional(page_validity, page_values) => {
                let max_def = page_validity.max_def();
                read_optional_values(
                    page_validity.definition_levels.by_ref(),
                    max_def,
                    page_values.values.by_ref().map(self.op),
                    values,
                    validity,
                    remaining,
                )
            }
            State::Required(page) => {
                values.extend(page.values.by_ref().map(self.op).take(remaining));
            }
            //State::OptionalDictionary(page) => todo!(),
            //State::RequiredDictionary(page) => todo!(),
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
    init: InitNested,
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
    pub fn new(iter: I, init: InitNested, data_type: DataType, chunk_size: usize, op: F) -> Self {
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
            MaybeNext::Some(Ok((nested, values, validity))) => {
                Some(Ok((nested, finish(&self.data_type, values, validity))))
            }
            MaybeNext::Some(Err(e)) => Some(Err(e)),
            MaybeNext::None => None,
            MaybeNext::More => self.next(),
        }
    }
}
