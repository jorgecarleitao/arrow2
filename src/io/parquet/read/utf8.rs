use parquet2::{
    encoding::{get_length, hybrid_rle, Encoding},
    metadata::ColumnDescriptor,
    read::{decompress_page, BinaryPageDict, Page},
    serialization::levels,
};

use crate::{
    array::{Offset, Utf8Array},
    bitmap::{BitmapIter, MutableBitmap},
    buffer::MutableBuffer,
    error::{ArrowError, Result},
};

use super::utils;

/// Assumptions: No rep levels
fn read_dict_buffer<O: Offset>(
    buffer: &[u8],
    length: u32,
    dict: &BinaryPageDict,
    _has_validity: bool,
    offsets: &mut MutableBuffer<O>,
    values: &mut MutableBuffer<u8>,
    validity: &mut MutableBitmap,
) {
    let length = length as usize;
    let dict_values = dict.values();
    let dict_offsets = dict.offsets();
    let mut last_offset = *offsets.as_slice_mut().last().unwrap();

    // split in two buffers: def_levels and data
    let def_level_buffer_length = get_length(buffer) as usize;
    let validity_buffer = &buffer[4..4 + def_level_buffer_length];
    let indices_buffer = &buffer[4 + def_level_buffer_length..];

    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let non_null_indices_len = (buffer.len() * 8 / bit_width as usize) as u32;
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
                        let dict_offset_i = dict_offsets[index] as usize;
                        let dict_offset_ip1 = dict_offsets[index + 1] as usize;
                        let length = dict_offset_ip1 - dict_offset_i;
                        last_offset += O::from_usize(length).unwrap();
                        values.extend_from_slice(&dict_values[dict_offset_i..dict_offset_ip1]);
                    };
                    offsets.push(last_offset);
                }
            }
            hybrid_rle::HybridEncoded::Rle(value, additional) => {
                let is_set = value[0] == 1;
                validity.extend_constant(additional, is_set);
                if is_set {
                    (0..additional).for_each(|_| {
                        let index = indices.next().unwrap() as usize;
                        let dict_offset_i = dict_offsets[index] as usize;
                        let dict_offset_ip1 = dict_offsets[index + 1] as usize;
                        let length = dict_offset_ip1 - dict_offset_i;
                        last_offset += O::from_usize(length).unwrap();
                        values.extend_from_slice(&dict_values[dict_offset_i..dict_offset_ip1]);
                    })
                } else {
                    offsets.extend_constant(additional, last_offset)
                }
            }
        }
    }
}

fn read_buffer<O: Offset>(
    buffer: &[u8],
    length: u32,
    _has_validity: bool,
    offsets: &mut MutableBuffer<O>,
    values: &mut MutableBuffer<u8>,
    validity: &mut MutableBitmap,
) {
    let length = length as usize;
    let mut last_offset = *offsets.as_slice_mut().last().unwrap();

    // split in two buffers: def_levels and data
    let def_level_buffer_length = get_length(buffer) as usize;
    let validity_buffer = &buffer[4..4 + def_level_buffer_length];
    let values_buffer = &buffer[4 + def_level_buffer_length..];

    // values_buffer: first 4 bytes are len, remaining is values
    let mut values_iterator = utils::BinaryIter::new(values_buffer);

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
                    let value = values_iterator.next().unwrap();
                    last_offset += O::from_usize(value.len()).unwrap();
                    offsets.push(last_offset);
                    values.extend_from_slice(value);
                }
            }
            hybrid_rle::HybridEncoded::Rle(value, additional) => {
                let is_set = value[0] == 1;
                validity.extend_constant(additional, is_set);
                if is_set {
                    (0..additional).for_each(|_| {
                        let value = values_iterator.next().unwrap();
                        last_offset += O::from_usize(value.len()).unwrap();
                        offsets.push(last_offset);
                        values.extend_from_slice(value)
                    })
                } else {
                    offsets.extend_constant(additional, last_offset)
                }
            }
        }
    }
}

pub fn iter_to_array<O, I, E>(mut iter: I, descriptor: &ColumnDescriptor) -> Result<Utf8Array<O>>
where
    ArrowError: From<E>,
    O: Offset,
    I: Iterator<Item = std::result::Result<Page, E>>,
{
    // todo: push metadata from the file to get this capacity
    let capacity = 0;
    let mut values = MutableBuffer::<u8>::with_capacity(0);
    let mut offsets = MutableBuffer::<O>::with_capacity(1 + capacity);
    offsets.push(O::default());
    let mut validity = MutableBitmap::with_capacity(capacity);
    iter.try_for_each(|page| {
        extend_from_page(page?, &descriptor, &mut offsets, &mut values, &mut validity)
    })?;

    Ok(Utf8Array::from_data(
        offsets.into(),
        values.into(),
        validity.into(),
    ))
}

fn extend_from_page<O: Offset>(
    page: Page,
    descriptor: &ColumnDescriptor,
    offsets: &mut MutableBuffer<O>,
    values: &mut MutableBuffer<u8>,
    validity: &mut MutableBitmap,
) -> Result<()> {
    let page = decompress_page(page).map_err(ArrowError::from_external_error)?;
    assert_eq!(descriptor.max_rep_level(), 0);
    assert_eq!(descriptor.max_def_level(), 1);
    let has_validity = descriptor.max_def_level() == 1;
    match page {
        Page::V1(page) => {
            assert_eq!(page.def_level_encoding, Encoding::Rle);
            match (&page.encoding, &page.dictionary_page) {
                (Encoding::PlainDictionary, Some(dict)) => read_dict_buffer::<O>(
                    &page.buf,
                    page.num_values,
                    dict.as_any().downcast_ref().unwrap(),
                    has_validity,
                    offsets,
                    values,
                    validity,
                ),
                (Encoding::Plain, None) => read_buffer::<O>(
                    &page.buf,
                    page.num_values,
                    has_validity,
                    offsets,
                    values,
                    validity,
                ),
                (encoding, None) => {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "Encoding {:?} not yet implemented for Binary",
                        encoding
                    )))
                }
                _ => todo!(),
            }
        }
        Page::V2(_) => todo!(),
    };
    Ok(())
}
