use parquet2::{
    encoding::{hybrid_rle, Encoding},
    metadata::ColumnDescriptor,
    read::{decompress_page, CompressedPage, FixedLenByteArrayPageDict, Page},
    serialization::read::levels,
};

use crate::{
    array::FixedSizeBinaryArray,
    bitmap::{BitmapIter, MutableBitmap},
    buffer::MutableBuffer,
    datatypes::DataType,
    error::{ArrowError, Result},
};

use super::utils;

/// Assumptions: No rep levels
#[allow(clippy::too_many_arguments)]
pub(crate) fn read_dict_buffer(
    validity_buffer: &[u8],
    indices_buffer: &[u8],
    length: u32,
    size: i32,
    dict: &FixedLenByteArrayPageDict,
    values: &mut MutableBuffer<u8>,
    validity: &mut MutableBitmap,
) {
    let length = length as usize;
    let size = size as usize;
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
                    if is_valid {
                        let index = indices.next().unwrap() as usize;
                        values.extend_from_slice(&dict_values[index..(index + 1) * size]);
                    } else {
                        values.extend_constant(size, 0);
                    }
                }
            }
            hybrid_rle::HybridEncoded::Rle(value, additional) => {
                let is_set = value[0] == 1;
                validity.extend_constant(additional, is_set);
                if is_set {
                    (0..additional).for_each(|_| {
                        let index = indices.next().unwrap() as usize;
                        values.extend_from_slice(&dict_values[index..(index + 1) * size]);
                    })
                } else {
                    values.extend_constant(additional * size, 0)
                }
            }
        }
    }
}

pub(crate) fn read_optional(
    validity_buffer: &[u8],
    values_buffer: &[u8],
    length: u32,
    size: i32,
    values: &mut MutableBuffer<u8>,
    validity: &mut MutableBitmap,
) {
    let length = length as usize;
    let size = size as usize;

    assert_eq!(values_buffer.len() % size, 0);
    let mut values_iterator = values_buffer.chunks_exact(size);

    let validity_iterator = hybrid_rle::Decoder::new(&validity_buffer, 1);

    validity.reserve(length);
    values.reserve(length * size);
    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed) => {
                // the pack may contain more items than needed.
                let remaining = length - values.len();
                let len = std::cmp::min(packed.len() * 8, remaining);
                for is_valid in BitmapIter::new(packed, 0, len) {
                    validity.push(is_valid);
                    if is_valid {
                        let value = values_iterator.next().unwrap();
                        values.extend_from_slice(value);
                    } else {
                        values.extend_constant(size, 0)
                    }
                }
            }
            hybrid_rle::HybridEncoded::Rle(value, additional) => {
                let is_set = value[0] == 1;
                validity.extend_constant(additional, is_set);
                if is_set {
                    (0..additional).for_each(|_| {
                        let value = values_iterator.next().unwrap();
                        values.extend_from_slice(value)
                    })
                } else {
                    values.extend_constant(additional * size, 0)
                }
            }
        }
    }
}

pub(crate) fn read_required(buffer: &[u8], length: u32, size: i32, values: &mut MutableBuffer<u8>) {
    assert_eq!(buffer.len(), length as usize * size as usize);
    values.extend_from_slice(buffer);
}

pub fn iter_to_array<I, E>(
    mut iter: I,
    size: i32,
    descriptor: &ColumnDescriptor,
) -> Result<FixedSizeBinaryArray>
where
    ArrowError: From<E>,
    I: Iterator<Item = std::result::Result<CompressedPage, E>>,
{
    let capacity = 0;
    let mut values = MutableBuffer::<u8>::with_capacity(0);
    let mut validity = MutableBitmap::with_capacity(capacity);
    iter.try_for_each(|page| {
        extend_from_page(page?, size, &descriptor, &mut values, &mut validity)
    })?;

    Ok(FixedSizeBinaryArray::from_data(
        DataType::FixedSizeBinary(size),
        values.into(),
        validity.into(),
    ))
}

pub(crate) fn extend_from_page(
    page: CompressedPage,
    size: i32,
    descriptor: &ColumnDescriptor,
    values: &mut MutableBuffer<u8>,
    validity: &mut MutableBitmap,
) -> Result<()> {
    let page = decompress_page(page)?;
    assert_eq!(descriptor.max_rep_level(), 0);
    assert!(descriptor.max_def_level() <= 1);
    let is_optional = descriptor.max_def_level() == 1;
    match page {
        Page::V1(page) => {
            assert_eq!(page.header.definition_level_encoding, Encoding::Rle);

            match (&page.header.encoding, &page.dictionary_page, is_optional) {
                (Encoding::PlainDictionary, Some(dict), true) => {
                    // split in two buffers: def_levels and data
                    let (validity_buffer, values_buffer) = utils::split_buffer_v1(&page.buffer);
                    read_dict_buffer(
                        validity_buffer,
                        values_buffer,
                        page.header.num_values as u32,
                        size,
                        dict.as_any().downcast_ref().unwrap(),
                        values,
                        validity,
                    )
                }
                (Encoding::Plain, None, true) => {
                    // split in two buffers: def_levels and data
                    let (validity_buffer, values_buffer) = utils::split_buffer_v1(&page.buffer);
                    read_optional(
                        validity_buffer,
                        values_buffer,
                        page.header.num_values as u32,
                        size,
                        values,
                        validity,
                    )
                }
                (Encoding::Plain, None, false) => {
                    read_required(&page.buffer, page.header.num_values as u32, size, values)
                }
                _ => {
                    return Err(utils::not_implemented(
                        &page.header.encoding,
                        is_optional,
                        page.dictionary_page.is_some(),
                        "V1",
                        "Binary",
                    ))
                }
            }
        }
        Page::V2(page) => {
            let def_level_buffer_length = page.header.definition_levels_byte_length as usize;

            match (&page.header.encoding, &page.dictionary_page, is_optional) {
                (Encoding::PlainDictionary, Some(dict), true) => {
                    let (validity_buffer, values_buffer) =
                        utils::split_buffer_v2(&page.buffer, def_level_buffer_length);
                    read_dict_buffer(
                        validity_buffer,
                        values_buffer,
                        page.header.num_values as u32,
                        size,
                        dict.as_any().downcast_ref().unwrap(),
                        values,
                        validity,
                    )
                }
                (Encoding::Plain, None, true) => {
                    let (validity_buffer, values_buffer) =
                        utils::split_buffer_v2(&page.buffer, def_level_buffer_length);
                    read_optional(
                        validity_buffer,
                        values_buffer,
                        page.header.num_values as u32,
                        size,
                        values,
                        validity,
                    )
                }
                (Encoding::Plain, None, false) => {
                    read_required(&page.buffer, page.header.num_values as u32, size, values)
                }
                _ => {
                    return Err(utils::not_implemented(
                        &page.header.encoding,
                        is_optional,
                        page.dictionary_page.is_some(),
                        "V2",
                        "Binary",
                    ))
                }
            }
        }
    };
    Ok(())
}
