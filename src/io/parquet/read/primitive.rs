use std::{convert::TryInto, hint::unreachable_unchecked};

use parquet2::{
    encoding::{hybrid_rle, Encoding},
    read::{Page, PageHeader, PrimitivePageDict, StreamingIterator},
    serialization::read::levels,
    types::NativeType,
};

use super::utils;
use super::{ColumnChunkMetaData, ColumnDescriptor};
use crate::{
    array::PrimitiveArray,
    bitmap::{BitmapIter, MutableBitmap},
    buffer::MutableBuffer,
    datatypes::DataType,
    error::{ArrowError, Result},
    trusted_len::TrustedLen,
    types::NativeType as ArrowNativeType,
};

struct ExactChunksIter<'a, T: NativeType> {
    chunks: std::slice::ChunksExact<'a, u8>,
    phantom: std::marker::PhantomData<T>,
}

impl<'a, T: NativeType> ExactChunksIter<'a, T> {
    #[inline]
    pub fn new(slice: &'a [u8]) -> Self {
        assert_eq!(slice.len() % std::mem::size_of::<T>(), 0);
        let chunks = slice.chunks_exact(std::mem::size_of::<T>());
        Self {
            chunks,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, T: NativeType> Iterator for ExactChunksIter<'a, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.chunks.next().map(|chunk| {
            let chunk: <T as NativeType>::Bytes = match chunk.try_into() {
                Ok(v) => v,
                Err(_) => unsafe { unreachable_unchecked() },
            };
            T::from_le_bytes(chunk)
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.chunks.size_hint()
    }
}

unsafe impl<'a, T: NativeType> TrustedLen for ExactChunksIter<'a, T> {}

fn read_dict_buffer_optional<T, A, F>(
    buffer: &[u8],
    length: u32,
    dict: &PrimitivePageDict<T>,
    values: &mut MutableBuffer<A>,
    validity: &mut MutableBitmap,
    op: F,
) where
    T: NativeType,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
    let (validity_buffer, indices_buffer) = utils::split_buffer_v1(buffer);

    let length = length as usize;
    let dict_values = dict.values();

    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let non_null_indices_len = (indices_buffer.len() * 8 / bit_width as usize) as u32;
    let mut indices =
        levels::rle_decode(&indices_buffer, bit_width as u32, non_null_indices_len).into_iter();

    let validity_iterator = hybrid_rle::Decoder::new(&validity_buffer, 1);

    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed) => {
                let remaining = length - values.len();
                let len = std::cmp::min(packed.len() * 8, remaining);
                for is_valid in BitmapIter::new(packed, 0, len) {
                    validity.push(is_valid);
                    let value = if is_valid {
                        op(dict_values[indices.next().unwrap() as usize])
                    } else {
                        A::default()
                    };
                    values.push(value);
                }
            }
            hybrid_rle::HybridEncoded::Rle(value, additional) => {
                let is_set = value[0] == 1;
                validity.extend_constant(additional, is_set);
                if is_set {
                    (0..additional).for_each(|_| {
                        let index = indices.next().unwrap() as usize;
                        let value = op(dict_values[index]);
                        values.push(value)
                    })
                } else {
                    values.extend_constant(additional, A::default())
                }
            }
        }
    }
}

fn read_nullable<T, A, F>(
    validity_buffer: &[u8],
    values_buffer: &[u8],
    length: u32,
    values: &mut MutableBuffer<A>,
    validity: &mut MutableBitmap,
    op: F,
) where
    T: NativeType,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
    let length = length as usize;

    let mut chunks = ExactChunksIter::<T>::new(values_buffer);

    let validity_iterator = hybrid_rle::Decoder::new(&validity_buffer, 1);

    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed) => {
                // the pack may contain more items than needed.
                let remaining = length - values.len();
                let len = std::cmp::min(packed.len() * 8, remaining);
                for is_valid in BitmapIter::new(packed, 0, len) {
                    validity.push(is_valid);
                    let value = if is_valid {
                        op(chunks.next().unwrap())
                    } else {
                        A::default()
                    };
                    values.push(value);
                }
            }
            hybrid_rle::HybridEncoded::Rle(value, additional) => {
                let is_set = value[0] == 1;
                validity.extend_constant(additional, is_set);
                if is_set {
                    (0..additional).for_each(|_| {
                        let value = op(chunks.next().unwrap());
                        values.push(value)
                    })
                } else {
                    values.extend_constant(additional, A::default())
                }
            }
        }
    }
}

fn read_required<T, A, F>(values_buffer: &[u8], length: u32, values: &mut MutableBuffer<A>, op: F)
where
    T: NativeType,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
    assert_eq!(
        values_buffer.len(),
        length as usize * std::mem::size_of::<T>()
    );
    let iterator = ExactChunksIter::<T>::new(values_buffer);

    let iterator = iterator.map(|value| op(value));

    values.extend_from_trusted_len_iter(iterator);
}

pub fn iter_to_array<T, A, I, E, F>(
    mut iter: I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
    op: F,
) -> Result<PrimitiveArray<A>>
where
    ArrowError: From<E>,
    T: NativeType,
    E: Clone,
    A: ArrowNativeType,
    F: Copy + Fn(T) -> A,
    I: StreamingIterator<Item = std::result::Result<Page, E>>,
{
    let capacity = metadata.num_values() as usize;
    let mut values = MutableBuffer::<A>::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);
    while let Some(page) = iter.next() {
        extend_from_page(
            page.as_ref().map_err(|x| x.clone())?,
            metadata.descriptor(),
            &mut values,
            &mut validity,
            op,
        )?
    }

    Ok(PrimitiveArray::from_data(
        data_type,
        values.into(),
        validity.into(),
    ))
}

fn extend_from_page<T, A, F>(
    page: &Page,
    descriptor: &ColumnDescriptor,
    values: &mut MutableBuffer<A>,
    validity: &mut MutableBitmap,
    op: F,
) -> Result<()>
where
    T: NativeType,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
    assert_eq!(descriptor.max_rep_level(), 0);
    let is_optional = descriptor.max_def_level() == 1;
    match page.header() {
        PageHeader::V1(header) => {
            assert_eq!(header.definition_level_encoding, Encoding::Rle);

            match (&page.encoding(), page.dictionary_page(), is_optional) {
                (Encoding::PlainDictionary, Some(dict), true) => read_dict_buffer_optional(
                    page.buffer(),
                    page.num_values() as u32,
                    dict.as_any().downcast_ref().unwrap(),
                    values,
                    validity,
                    op,
                ),
                (Encoding::Plain, None, true) => {
                    let (validity_buffer, values_buffer) = utils::split_buffer_v1(page.buffer());
                    read_nullable(
                        validity_buffer,
                        values_buffer,
                        page.num_values() as u32,
                        values,
                        validity,
                        op,
                    )
                }
                (Encoding::Plain, None, false) => {
                    read_required(page.buffer(), page.num_values() as u32, values, op)
                }
                _ => {
                    return Err(utils::not_implemented(
                        &page.encoding(),
                        is_optional,
                        page.dictionary_page().is_some(),
                        "V1",
                        "primitive",
                    ))
                }
            }
        }
        PageHeader::V2(header) => match (&page.encoding(), page.dictionary_page(), is_optional) {
            (Encoding::PlainDictionary, Some(dict), true) => read_dict_buffer_optional(
                page.buffer(),
                page.num_values() as u32,
                dict.as_any().downcast_ref().unwrap(),
                values,
                validity,
                op,
            ),
            (Encoding::Plain, None, true) => {
                let def_level_buffer_length = header.definition_levels_byte_length as usize;
                let (validity_buffer, values_buffer) =
                    utils::split_buffer_v2(page.buffer(), def_level_buffer_length);
                read_nullable(
                    validity_buffer,
                    values_buffer,
                    page.num_values() as u32,
                    values,
                    validity,
                    op,
                )
            }
            (Encoding::Plain, None, false) => {
                read_required::<T, A, F>(page.buffer(), page.num_values() as u32, values, op)
            }
            _ => {
                return Err(utils::not_implemented(
                    &page.encoding(),
                    is_optional,
                    page.dictionary_page().is_some(),
                    "V2",
                    "primitive",
                ))
            }
        },
    };
    Ok(())
}
