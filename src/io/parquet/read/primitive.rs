use parquet2::{
    encoding::{hybrid_rle, Encoding},
    metadata::ColumnDescriptor,
    read::{decompress_page, CompressedPage, Page, PrimitivePageDict},
    serialization::read::levels,
    types,
    types::NativeType,
};

use super::utils;
use crate::{
    array::PrimitiveArray,
    bitmap::{BitmapIter, MutableBitmap},
    buffer::MutableBuffer,
    datatypes::DataType,
    error::{ArrowError, Result},
    types::NativeType as ArrowNativeType,
};

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

    validity.reserve(length);
    values.reserve(length);
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

    let mut chunks = values_buffer.chunks_exact(std::mem::size_of::<T>());

    let validity_iterator = hybrid_rle::Decoder::new(&validity_buffer, 1);

    validity.reserve(length);
    values.reserve(length);
    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed) => {
                // the pack may contain more items than needed.
                let remaining = length - values.len();
                let len = std::cmp::min(packed.len() * 8, remaining);
                for is_valid in BitmapIter::new(packed, 0, len) {
                    validity.push(is_valid);
                    let value = if is_valid {
                        op(types::decode::<T>(chunks.next().unwrap()))
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
                        let value = op(types::decode::<T>(chunks.next().unwrap()));
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
    // todo: for little endian machines this can be replaced by `extend_from_slice`
    // via transmute.
    let length = length as usize;
    let size = std::mem::size_of::<T>();

    let chunks = values_buffer[..length * size].chunks_exact(size);

    let iter = chunks.map(|chunk| op(types::decode::<T>(chunk)));

    values.extend(iter);
}

pub fn iter_to_array<T, A, I, E, F>(
    mut iter: I,
    descriptor: &ColumnDescriptor,
    data_type: DataType,
    op: F,
) -> Result<PrimitiveArray<A>>
where
    ArrowError: From<E>,
    T: NativeType,
    A: ArrowNativeType,
    F: Copy + Fn(T) -> A,
    I: Iterator<Item = std::result::Result<CompressedPage, E>>,
{
    // todo: push metadata from the file to get this capacity
    let capacity = 0;
    let mut values = MutableBuffer::<A>::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);
    iter.try_for_each(|page| extend_from_page(page?, &descriptor, &mut values, &mut validity, op))?;

    Ok(PrimitiveArray::from_data(
        data_type,
        values.into(),
        validity.into(),
    ))
}

fn extend_from_page<T, A, F>(
    page: CompressedPage,
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
    let page = decompress_page(page)?;
    assert_eq!(descriptor.max_rep_level(), 0);
    let is_optional = descriptor.max_def_level() == 1;
    match page {
        Page::V1(page) => {
            assert_eq!(page.header.definition_level_encoding, Encoding::Rle);

            match (&page.header.encoding, &page.dictionary_page, is_optional) {
                (Encoding::PlainDictionary, Some(dict), true) => read_dict_buffer_optional(
                    &page.buffer,
                    page.header.num_values as u32,
                    dict.as_any().downcast_ref().unwrap(),
                    values,
                    validity,
                    op,
                ),
                (Encoding::Plain, None, true) => {
                    let (validity_buffer, values_buffer) = utils::split_buffer_v1(&page.buffer);
                    read_nullable(
                        validity_buffer,
                        values_buffer,
                        page.header.num_values as u32,
                        values,
                        validity,
                        op,
                    )
                }
                (Encoding::Plain, None, false) => {
                    read_required(&page.buffer, page.header.num_values as u32, values, op)
                }
                _ => {
                    return Err(utils::not_implemented(
                        &page.header.encoding,
                        is_optional,
                        page.dictionary_page.is_some(),
                        "V1",
                        "primitive",
                    ))
                }
            }
        }
        Page::V2(page) => match (&page.header.encoding, &page.dictionary_page, is_optional) {
            (Encoding::PlainDictionary, Some(dict), true) => read_dict_buffer_optional(
                &page.buffer,
                page.header.num_values as u32,
                dict.as_any().downcast_ref().unwrap(),
                values,
                validity,
                op,
            ),
            (Encoding::Plain, None, true) => {
                let def_level_buffer_length = page.header.definition_levels_byte_length as usize;
                let (validity_buffer, values_buffer) =
                    utils::split_buffer_v2(&page.buffer, def_level_buffer_length);
                read_nullable(
                    validity_buffer,
                    values_buffer,
                    page.header.num_values as u32,
                    values,
                    validity,
                    op,
                )
            }
            (Encoding::Plain, None, false) => {
                read_required::<T, A, F>(&page.buffer, page.header.num_values as u32, values, op)
            }
            _ => {
                return Err(utils::not_implemented(
                    &page.header.encoding,
                    is_optional,
                    page.dictionary_page.is_some(),
                    "V2",
                    "primitive",
                ))
            }
        },
    };
    Ok(())
}
