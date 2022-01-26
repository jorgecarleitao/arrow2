use std::collections::VecDeque;

use parquet2::{
    encoding::{hybrid_rle, Encoding},
    page::{DataPage, PrimitivePageDict},
    types::NativeType as ParquetNativeType,
};

use super::super::utils as other_utils;
use super::utils::chunks;
use super::ColumnDescriptor;
use crate::io::parquet::read::utils::{Decoder, OptionalPageValidity};
use crate::{
    array::PrimitiveArray,
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::Result,
    io::parquet::read::{utils::extend_from_decoder, DataPages},
    types::NativeType,
};

#[inline]
fn values_iter<'a, T, A, F>(
    indices_buffer: &'a [u8],
    dict_values: &'a [T],
    additional: usize,
    op: F,
) -> impl Iterator<Item = A> + 'a
where
    T: ParquetNativeType,
    A: NativeType,
    F: 'a + Fn(T) -> A,
{
    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let indices = hybrid_rle::HybridRleDecoder::new(indices_buffer, bit_width as u32, additional);
    indices
        .map(move |index| dict_values[index as usize])
        .map(op)
}

fn read_dict_buffer_optional<T, A, F>(
    validity_buffer: &[u8],
    indices_buffer: &[u8],
    additional: usize,
    dict: &PrimitivePageDict<T>,
    values: &mut Vec<A>,
    validity: &mut MutableBitmap,
    op: F,
) where
    T: ParquetNativeType,
    A: NativeType,
    F: Fn(T) -> A,
{
    let values_iterator = values_iter(indices_buffer, dict.values(), additional, op);

    let mut page_validity = OptionalPageValidity::new(validity_buffer, additional);

    extend_from_decoder(validity, &mut page_validity, None, values, values_iterator)
}

fn read_dict_buffer_required<T, A, F>(
    indices_buffer: &[u8],
    additional: usize,
    dict: &PrimitivePageDict<T>,
    values: &mut Vec<A>,
    validity: &mut MutableBitmap,
    op: F,
) where
    T: ParquetNativeType,
    A: NativeType,
    F: Fn(T) -> A,
{
    debug_assert_eq!(0, validity.len());
    let values_iterator = values_iter(indices_buffer, dict.values(), additional, op);
    values.extend(values_iterator);
}

fn read_nullable<T, A, F>(
    validity_buffer: &[u8],
    values_buffer: &[u8],
    additional: usize,
    values: &mut Vec<A>,
    validity: &mut MutableBitmap,
    op: F,
) where
    T: ParquetNativeType,
    A: NativeType,
    F: Fn(T) -> A,
{
    let values_iter = chunks(values_buffer).map(op);

    let mut page_validity = OptionalPageValidity::new(validity_buffer, additional);

    extend_from_decoder(validity, &mut page_validity, None, values, values_iter)
}

fn read_required<T, A, F>(values_buffer: &[u8], additional: usize, values: &mut Vec<A>, op: F)
where
    T: ParquetNativeType,
    A: NativeType,
    F: Fn(T) -> A,
{
    assert_eq!(values_buffer.len(), additional * std::mem::size_of::<T>());
    let iterator = chunks(values_buffer).map(op);

    values.extend(iterator);
}

pub fn extend_from_page<T, A, F>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    values: &mut Vec<A>,
    validity: &mut MutableBitmap,
    op: F,
) -> Result<()>
where
    T: ParquetNativeType,
    A: NativeType,
    F: Fn(T) -> A,
{
    let additional = page.num_values();

    assert_eq!(descriptor.max_rep_level(), 0);
    let is_optional = descriptor.max_def_level() == 1;

    let (_, validity_buffer, values_buffer, version) = other_utils::split_buffer(page, descriptor);

    match (&page.encoding(), page.dictionary_page(), is_optional) {
        (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true) => {
            read_dict_buffer_optional(
                validity_buffer,
                values_buffer,
                additional,
                dict.as_any().downcast_ref().unwrap(),
                values,
                validity,
                op,
            )
        }
        (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false) => {
            read_dict_buffer_required(
                values_buffer,
                additional,
                dict.as_any().downcast_ref().unwrap(),
                values,
                validity,
                op,
            )
        }
        // it can happen that there is a dictionary but the encoding is plain because
        // it falled back.
        (Encoding::Plain, _, true) => read_nullable(
            validity_buffer,
            values_buffer,
            additional,
            values,
            validity,
            op,
        ),
        (Encoding::Plain, _, false) => read_required(page.buffer(), additional, values, op),
        _ => {
            return Err(other_utils::not_implemented(
                &page.encoding(),
                is_optional,
                page.dictionary_page().is_some(),
                version,
                "primitive",
            ))
        }
    }
    Ok(())
}

#[derive(Debug)]
struct RequiredPage<'a, T, P, G, F>
where
    T: NativeType,
    P: ParquetNativeType,
    G: for<'b> Fn(&'b [u8]) -> P,
    F: Fn(P) -> T,
{
    values: std::iter::Map<std::iter::Map<std::slice::ChunksExact<'a, u8>, G>, F>,
    phantom: std::marker::PhantomData<P>,
}

impl<'a, T, P, G, F> RequiredPage<'a, T, P, G, F>
where
    T: NativeType,
    P: ParquetNativeType,
    G: for<'b> Fn(&'b [u8]) -> P,
    F: Fn(P) -> T,
{
    fn new(page: &'a DataPage, op1: G, op2: F) -> Self {
        assert_eq!(
            page.buffer().len(),
            page.num_values() * std::mem::size_of::<T>()
        );
        Self {
            phantom: Default::default(),
            values: page
                .buffer()
                .chunks_exact(std::mem::size_of::<P>())
                .map(op1)
                .map(op2),
        }
    }
}

#[derive(Debug)]
struct OptionalPage<'a, T, P, G, F>
where
    T: NativeType,
    P: ParquetNativeType,
    G: for<'b> Fn(&'b [u8]) -> P,
    F: Fn(P) -> T,
{
    values: std::iter::Map<std::iter::Map<std::slice::ChunksExact<'a, u8>, G>, F>,
    phantom: std::marker::PhantomData<P>,
    validity: OptionalPageValidity<'a>,
}

impl<'a, T, P, G, F> OptionalPage<'a, T, P, G, F>
where
    T: NativeType,
    P: ParquetNativeType,
    G: for<'b> Fn(&'b [u8]) -> P,
    F: Fn(P) -> T,
{
    fn new(page: &'a DataPage, op1: G, op2: F) -> Self {
        let (_, validity_buffer, values_buffer, _) =
            other_utils::split_buffer(page, page.descriptor());

        Self {
            values: values_buffer
                .chunks_exact(std::mem::size_of::<P>())
                .map(op1)
                .map(op2),
            phantom: Default::default(),
            validity: OptionalPageValidity::new(validity_buffer, page.num_values()),
        }
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
    P: ParquetNativeType,
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
struct RequiredDictionaryPage<'a, T, P, F>
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

impl<'a, T, P, F> RequiredDictionaryPage<'a, T, P, F>
where
    T: NativeType,
    P: ParquetNativeType,
    F: Fn(P) -> T,
{
    fn new(page: &'a DataPage, dict: &'a PrimitivePageDict<P>, op2: F) -> Self {
        let values = dict.values();
        let op1 = Box::new(move |index: u32| values[index as usize]) as Box<dyn Fn(u32) -> P>;

        let values = values_iter1(page.buffer(), page.num_values(), op1, op2);

        Self {
            phantom: Default::default(),
            values,
        }
    }
}

#[derive(Debug)]
struct OptionalDictionaryPage<'a, T, P, F>
where
    T: NativeType,
    P: ParquetNativeType,
    F: Fn(P) -> T,
{
    validity: OptionalPageValidity<'a>,
    values: std::iter::Map<
        std::iter::Map<hybrid_rle::HybridRleDecoder<'a>, Box<dyn Fn(u32) -> P + 'a>>,
        F,
    >,
    phantom: std::marker::PhantomData<P>,
}

impl<'a, T, P, F> OptionalDictionaryPage<'a, T, P, F>
where
    T: NativeType,
    P: ParquetNativeType,
    F: Fn(P) -> T,
{
    fn new(page: &'a DataPage, dict: &'a PrimitivePageDict<P>, op2: F) -> Self {
        let (_, validity_buffer, values_buffer, _) =
            other_utils::split_buffer(page, page.descriptor());

        let values = dict.values();
        let op1 = Box::new(move |index: u32| values[index as usize]) as Box<dyn Fn(u32) -> P>;
        let values = values_iter1(values_buffer, page.num_values(), op1, op2);

        Self {
            phantom: Default::default(),
            values,
            validity: OptionalPageValidity::new(validity_buffer, page.num_values()),
        }
    }
}

// The state of a `DataPage` of `Primitive` parquet primitive type
#[derive(Debug)]
enum PrimitivePageState<'a, T, P, G, F>
where
    T: NativeType,
    P: ParquetNativeType,
    G: Copy + for<'b> Fn(&'b [u8]) -> P,
    F: Copy + Fn(P) -> T,
{
    Optional(OptionalPage<'a, T, P, G, F>),
    Required(RequiredPage<'a, T, P, G, F>),
    RequiredDictionary(RequiredDictionaryPage<'a, T, P, F>),
    OptionalDictionary(OptionalDictionaryPage<'a, T, P, F>),
}

impl<'a, T, P, G, F> other_utils::PageState<'a> for PrimitivePageState<'a, T, P, G, F>
where
    T: NativeType,
    P: ParquetNativeType,
    G: Copy + for<'b> Fn(&'b [u8]) -> P,
    F: Copy + Fn(P) -> T,
{
    fn len(&self) -> usize {
        match self {
            PrimitivePageState::Optional(optional) => optional.validity.len(),
            PrimitivePageState::Required(required) => required.values.size_hint().0,
            PrimitivePageState::RequiredDictionary(required) => required.values.size_hint().0,
            PrimitivePageState::OptionalDictionary(optional) => optional.validity.len(),
        }
    }
}

fn build_state<'a, T, P, G, F>(
    page: &'a DataPage,
    is_optional: bool,
    op1: G,
    op2: F,
) -> Result<PrimitivePageState<'a, T, P, G, F>>
where
    T: NativeType,
    P: ParquetNativeType,
    G: Copy + for<'b> Fn(&'b [u8]) -> P,
    F: Copy + Fn(P) -> T,
{
    match (page.encoding(), page.dictionary_page(), is_optional) {
        (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false) => {
            let dict = dict.as_any().downcast_ref().unwrap();
            Ok(PrimitivePageState::RequiredDictionary(
                RequiredDictionaryPage::new(page, dict, op2),
            ))
        }
        (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true) => {
            let dict = dict.as_any().downcast_ref().unwrap();
            Ok(PrimitivePageState::OptionalDictionary(
                OptionalDictionaryPage::new(page, dict, op2),
            ))
        }
        (Encoding::Plain, None, true) => Ok(PrimitivePageState::Optional(OptionalPage::new(
            page, op1, op2,
        ))),
        (Encoding::Plain, None, false) => Ok(PrimitivePageState::Required(RequiredPage::new(
            page, op1, op2,
        ))),
        _ => Err(other_utils::not_implemented(
            &page.encoding(),
            is_optional,
            false,
            "any",
            "Primitive",
        )),
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
    phantom_g: std::marker::PhantomData<G>,
    phantom_f: std::marker::PhantomData<F>,
}

impl<'a, T, P, G, F> Default for PrimitiveDecoder<T, P, G, F>
where
    T: NativeType,
    P: ParquetNativeType,
    G: for<'b> Fn(&'b [u8]) -> P,
    F: Fn(P) -> T,
{
    #[inline]
    fn default() -> Self {
        Self {
            phantom: std::marker::PhantomData,
            phantom_p: std::marker::PhantomData,
            phantom_g: std::marker::PhantomData,
            phantom_f: std::marker::PhantomData,
        }
    }
}

impl<'a, T, P, G, F> other_utils::Decoder<'a, T, Vec<T>> for PrimitiveDecoder<T, P, G, F>
where
    T: NativeType,
    P: ParquetNativeType,
    G: Copy + for<'b> Fn(&'b [u8]) -> P,
    F: Copy + Fn(P) -> T,
{
    type State = PrimitivePageState<'a, T, P, G, F>;
    type Array = PrimitiveArray<T>;

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
            PrimitivePageState::Optional(page) => extend_from_decoder(
                validity,
                &mut page.validity,
                Some(remaining),
                values,
                &mut page.values,
            ),
            PrimitivePageState::Required(page) => {
                values.extend(page.values.by_ref().take(remaining));
            }
            PrimitivePageState::OptionalDictionary(page) => extend_from_decoder(
                validity,
                &mut page.validity,
                Some(remaining),
                values,
                &mut page.values,
            ),
            PrimitivePageState::RequiredDictionary(page) => {
                values.extend(page.values.by_ref().take(remaining));
            }
        }
    }

    fn finish(data_type: DataType, values: Vec<T>, validity: MutableBitmap) -> Self::Array {
        let data_type = match data_type {
            DataType::Dictionary(_, values, _) => values.as_ref().clone(),
            _ => data_type,
        };
        PrimitiveArray::from_data(data_type, values.into(), validity.into())
    }
}

/// An iterator adapter over [`DataPages`] assumed to be encoded as boolean arrays
#[derive(Debug)]
pub struct PrimitiveArrayIterator<T, I, P, G, F>
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
    is_optional: bool,
    op1: G,
    op2: F,
    phantom: std::marker::PhantomData<P>,
}

impl<T, I, P, G, F> PrimitiveArrayIterator<T, I, P, G, F>
where
    I: DataPages,
    T: NativeType,

    P: ParquetNativeType,
    G: Copy + for<'b> Fn(&'b [u8]) -> P,
    F: Copy + Fn(P) -> T,
{
    pub fn new(
        iter: I,
        data_type: DataType,
        chunk_size: usize,
        is_optional: bool,
        op1: G,
        op2: F,
    ) -> Self {
        Self {
            iter,
            data_type,
            items: VecDeque::new(),
            chunk_size,
            is_optional,
            op1,
            op2,
            phantom: Default::default(),
        }
    }
}

impl<T, I, P, G, F> Iterator for PrimitiveArrayIterator<T, I, P, G, F>
where
    I: DataPages,
    T: NativeType,
    P: ParquetNativeType,
    G: Copy + for<'b> Fn(&'b [u8]) -> P,
    F: Copy + Fn(P) -> T,
{
    type Item = Result<PrimitiveArray<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        // back[a1, a2, a3, ...]front
        if self.items.len() > 1 {
            return self.items.pop_back().map(|(values, validity)| {
                Ok(PrimitiveDecoder::<T, P, G, F>::finish(
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
                let maybe_array = {
                    // there is a new page => consume the page from the start
                    let maybe_page = build_state(page, self.is_optional, self.op1, self.op2);
                    let page = match maybe_page {
                        Ok(page) => page,
                        Err(e) => return Some(Err(e)),
                    };

                    other_utils::extend_from_new_page::<PrimitiveDecoder<T, P, G, F>, _, _>(
                        page,
                        state,
                        &self.data_type,
                        self.chunk_size,
                        &mut self.items,
                        &PrimitiveDecoder::default(),
                    )
                };
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
                Some(Ok(PrimitiveDecoder::<T, P, G, F>::finish(
                    self.data_type.clone(),
                    values,
                    validity,
                )))
            }
        }
    }
}
