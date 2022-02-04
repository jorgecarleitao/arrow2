use std::collections::VecDeque;

use parquet2::{
    encoding::{hybrid_rle, Encoding},
    page::{DataPage, PrimitivePageDict},
    schema::Repetition,
    types::NativeType as ParquetNativeType,
};

use crate::{
    array::MutablePrimitiveArray, bitmap::MutableBitmap, datatypes::DataType, error::Result,
    types::NativeType,
};

use super::super::utils;
use super::super::utils::OptionalPageValidity;
use super::super::DataPages;

#[derive(Debug)]
pub(super) struct Values<'a, T, P, G, F>
where
    T: NativeType,
    P: ParquetNativeType,
    G: for<'b> Fn(&'b [u8]) -> P,
    F: Fn(P) -> T,
{
    pub values: std::iter::Map<std::iter::Map<std::slice::ChunksExact<'a, u8>, G>, F>,
    phantom: std::marker::PhantomData<P>,
}

impl<'a, T, P, G, F> Values<'a, T, P, G, F>
where
    T: NativeType,
    P: ParquetNativeType,
    G: for<'b> Fn(&'b [u8]) -> P,
    F: Fn(P) -> T,
{
    pub fn new(page: &'a DataPage, op1: G, op2: F) -> Self {
        let (_, _, values, _) = utils::split_buffer(page, page.descriptor());
        assert_eq!(values.len() % std::mem::size_of::<P>(), 0);
        Self {
            phantom: Default::default(),
            values: values
                .chunks_exact(std::mem::size_of::<P>())
                .map(op1)
                .map(op2),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.size_hint().0
    }
}

#[inline]
fn values_iter1<P, T, G, F>(
    indices_buffer: &[u8],
    additional: usize,
    op1: G,
    op2: F,
) -> std::iter::Map<std::iter::Map<hybrid_rle::HybridRleDecoder, G>, F>
where
    T: NativeType,
    G: Fn(u32) -> P,
    F: Fn(P) -> T,
{
    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let indices = hybrid_rle::HybridRleDecoder::new(indices_buffer, bit_width as u32, additional);
    indices.map(op1).map(op2)
}

#[derive(Debug)]
pub(super) struct ValuesDictionary<'a, T, P, F>
where
    T: NativeType,
    P: ParquetNativeType,
    F: Fn(P) -> T,
{
    values: std::iter::Map<
        std::iter::Map<hybrid_rle::HybridRleDecoder<'a>, Box<dyn Fn(u32) -> P + 'a>>,
        F,
    >,
    phantom: std::marker::PhantomData<P>,
}

impl<'a, T, P, F> ValuesDictionary<'a, T, P, F>
where
    T: NativeType,
    P: ParquetNativeType,
    F: Fn(P) -> T,
{
    fn new(data: &'a [u8], length: usize, dict: &'a PrimitivePageDict<P>, op2: F) -> Self {
        let values = dict.values();
        let op1 = Box::new(move |index: u32| values[index as usize]) as Box<dyn Fn(u32) -> P>;

        let values = values_iter1(data, length, op1, op2);

        Self {
            phantom: Default::default(),
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
enum State<'a, T, P, G, F>
where
    T: NativeType,
    P: ParquetNativeType,
    G: Copy + for<'b> Fn(&'b [u8]) -> P,
    F: Copy + Fn(P) -> T,
{
    Optional(OptionalPageValidity<'a>, Values<'a, T, P, G, F>),
    Required(Values<'a, T, P, G, F>),
    RequiredDictionary(ValuesDictionary<'a, T, P, F>),
    OptionalDictionary(OptionalPageValidity<'a>, ValuesDictionary<'a, T, P, F>),
}

impl<'a, T, P, G, F> utils::PageState<'a> for State<'a, T, P, G, F>
where
    T: NativeType,
    P: ParquetNativeType,
    G: Copy + for<'b> Fn(&'b [u8]) -> P,
    F: Copy + Fn(P) -> T,
{
    fn len(&self) -> usize {
        match self {
            State::Optional(optional, _) => optional.len(),
            State::Required(values) => values.len(),
            State::RequiredDictionary(values) => values.len(),
            State::OptionalDictionary(optional, _) => optional.len(),
        }
    }
}

#[derive(Debug)]
struct PrimitiveDecoder<T, P, G, F>
where
    T: NativeType,
    P: ParquetNativeType,
    G: for<'b> Fn(&'b [u8]) -> P,
    F: Fn(P) -> T,
{
    phantom: std::marker::PhantomData<T>,
    phantom_p: std::marker::PhantomData<P>,
    op1: G,
    op2: F,
}

impl<'a, T, P, G, F> PrimitiveDecoder<T, P, G, F>
where
    T: NativeType,
    P: ParquetNativeType,
    G: for<'b> Fn(&'b [u8]) -> P,
    F: Fn(P) -> T,
{
    #[inline]
    fn new(op1: G, op2: F) -> Self {
        Self {
            phantom: std::marker::PhantomData,
            phantom_p: std::marker::PhantomData,
            op1,
            op2,
        }
    }
}

impl<'a, T, P, G, F> utils::Decoder<'a, T, Vec<T>> for PrimitiveDecoder<T, P, G, F>
where
    T: NativeType,
    P: ParquetNativeType,
    G: Copy + for<'b> Fn(&'b [u8]) -> P,
    F: Copy + Fn(P) -> T,
{
    type State = State<'a, T, P, G, F>;

    fn build_state(&self, page: &'a DataPage) -> Result<Self::State> {
        let is_optional =
            page.descriptor().type_().get_basic_info().repetition() == &Repetition::Optional;

        match (page.encoding(), page.dictionary_page(), is_optional) {
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false) => {
                let dict = dict.as_any().downcast_ref().unwrap();
                Ok(State::RequiredDictionary(ValuesDictionary::new(
                    page.buffer(),
                    page.num_values(),
                    dict,
                    self.op2,
                )))
            }
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true) => {
                let dict = dict.as_any().downcast_ref().unwrap();

                let (_, _, values_buffer, _) = utils::split_buffer(page, page.descriptor());

                Ok(State::OptionalDictionary(
                    OptionalPageValidity::new(page),
                    ValuesDictionary::new(values_buffer, page.num_values(), dict, self.op2),
                ))
            }
            (Encoding::Plain, None, true) => {
                let validity = OptionalPageValidity::new(page);
                let values = Values::new(page, self.op1, self.op2);

                Ok(State::Optional(validity, values))
            }
            (Encoding::Plain, None, false) => {
                Ok(State::Required(Values::new(page, self.op1, self.op2)))
            }
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
        state: &mut Self::State,
        values: &mut Vec<T>,
        validity: &mut MutableBitmap,
        remaining: usize,
    ) {
        match state {
            State::Optional(page_validity, page_values) => utils::extend_from_decoder(
                validity,
                page_validity,
                Some(remaining),
                values,
                &mut page_values.values,
            ),
            State::Required(page) => {
                values.extend(page.values.by_ref().take(remaining));
            }
            State::OptionalDictionary(page_validity, page_values) => utils::extend_from_decoder(
                validity,
                page_validity,
                Some(remaining),
                values,
                &mut page_values.values,
            ),
            State::RequiredDictionary(page) => {
                values.extend(page.values.by_ref().take(remaining));
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
pub struct Iter<T, I, P, G, F>
where
    I: DataPages,
    T: NativeType,
    P: ParquetNativeType,
    G: for<'b> Fn(&'b [u8]) -> P,
    F: Fn(P) -> T,
{
    iter: I,
    data_type: DataType,
    items: VecDeque<(Vec<T>, MutableBitmap)>,
    chunk_size: usize,
    op1: G,
    op2: F,
    phantom: std::marker::PhantomData<P>,
}

impl<T, I, P, G, F> Iter<T, I, P, G, F>
where
    I: DataPages,
    T: NativeType,

    P: ParquetNativeType,
    G: Copy + for<'b> Fn(&'b [u8]) -> P,
    F: Copy + Fn(P) -> T,
{
    pub fn new(iter: I, data_type: DataType, chunk_size: usize, op1: G, op2: F) -> Self {
        Self {
            iter,
            data_type,
            items: VecDeque::new(),
            chunk_size,
            op1,
            op2,
            phantom: Default::default(),
        }
    }
}

impl<T, I, P, G, F> Iterator for Iter<T, I, P, G, F>
where
    I: DataPages,
    T: NativeType,
    P: ParquetNativeType,
    G: Copy + for<'b> Fn(&'b [u8]) -> P,
    F: Copy + Fn(P) -> T,
{
    type Item = Result<MutablePrimitiveArray<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_state = utils::next(
            &mut self.iter,
            &mut self.items,
            self.chunk_size,
            &PrimitiveDecoder::new(self.op1, self.op2),
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
