use parquet2::{
    encoding::{delta_length_byte_array, hybrid_rle, Encoding},
    metadata::ColumnDescriptor,
    page::{BinaryPageDict, DataPage},
};

use crate::{
    array::Offset,
    bitmap::{utils::BitmapIter, MutableBitmap},
    error::Result,
};

use super::super::utils;

/// Assumptions: No rep levels
#[allow(clippy::too_many_arguments)]
fn read_dict_buffer<O: Offset>(
    validity_buffer: &[u8],
    indices_buffer: &[u8],
    additional: usize,
    dict: &BinaryPageDict,
    offsets: &mut Vec<O>,
    values: &mut Vec<u8>,
    validity: &mut MutableBitmap,
) {
    let length = (offsets.len() - 1) + additional;
    let dict_values = dict.values();
    let dict_offsets = dict.offsets();
    let mut last_offset = *offsets.as_mut_slice().last().unwrap();

    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let mut indices = hybrid_rle::HybridRleDecoder::new(indices_buffer, bit_width as u32, length);

    let validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed) => {
                let remaining = length - (offsets.len() - 1);
                let len = std::cmp::min(packed.len() * 8, remaining);
                for is_valid in BitmapIter::new(packed, 0, len) {
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
                validity.extend_from_slice(packed, 0, len);
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
                        offsets.push(last_offset);
                        values.extend_from_slice(&dict_values[dict_offset_i..dict_offset_ip1]);
                    })
                } else {
                    offsets.resize(values.len() + additional, last_offset);
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn read_dict_required<O: Offset>(
    indices_buffer: &[u8],
    additional: usize,
    dict: &BinaryPageDict,
    offsets: &mut Vec<O>,
    values: &mut Vec<u8>,
    validity: &mut MutableBitmap,
) {
    let dict_values = dict.values();
    let dict_offsets = dict.offsets();
    let mut last_offset = *offsets.as_mut_slice().last().unwrap();

    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let indices = hybrid_rle::HybridRleDecoder::new(indices_buffer, bit_width as u32, additional);

    for index in indices {
        let index = index as usize;
        let dict_offset_i = dict_offsets[index] as usize;
        let dict_offset_ip1 = dict_offsets[index + 1] as usize;
        let length = dict_offset_ip1 - dict_offset_i;
        last_offset += O::from_usize(length).unwrap();
        offsets.push(last_offset);
        values.extend_from_slice(&dict_values[dict_offset_i..dict_offset_ip1]);
    }
    validity.extend_constant(additional, true);
}

fn read_delta_optional<O: Offset>(
    validity_buffer: &[u8],
    values_buffer: &[u8],
    additional: usize,
    offsets: &mut Vec<O>,
    values: &mut Vec<u8>,
    validity: &mut MutableBitmap,
) {
    let length = (offsets.len() - 1) + additional;
    let mut last_offset = *offsets.as_mut_slice().last().unwrap();

    // values_buffer: first 4 bytes are len, remaining is values
    let mut values_iterator = delta_length_byte_array::Decoder::new(values_buffer);

    let validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    // offsets:
    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed) => {
                // the pack may contain more items than needed.
                let remaining = length - (offsets.len() - 1);
                let len = std::cmp::min(packed.len() * 8, remaining);
                for is_valid in BitmapIter::new(packed, 0, len) {
                    if is_valid {
                        let value = values_iterator.next().unwrap() as usize;
                        last_offset += O::from_usize(value).unwrap();
                    }
                    offsets.push(last_offset);
                }
                validity.extend_from_slice(packed, 0, len);
            }
            hybrid_rle::HybridEncoded::Rle(value, additional) => {
                let is_set = value[0] == 1;
                validity.extend_constant(additional, is_set);
                if is_set {
                    (0..additional).for_each(|_| {
                        let value = values_iterator.next().unwrap() as usize;
                        last_offset += O::from_usize(value).unwrap();
                        offsets.push(last_offset);
                    })
                } else {
                    offsets.resize(values.len() + additional, last_offset);
                }
            }
        }
    }

    // values:
    let new_values = values_iterator.into_values();
    values.extend_from_slice(new_values);
}

fn read_plain_optional<O: Offset>(
    validity_buffer: &[u8],
    values_buffer: &[u8],
    additional: usize,
    offsets: &mut Vec<O>,
    values: &mut Vec<u8>,
    validity: &mut MutableBitmap,
) {
    let length = (offsets.len() - 1) + additional;
    let mut last_offset = *offsets.as_mut_slice().last().unwrap();

    // values_buffer: first 4 bytes are len, remaining is values
    let mut values_iterator = utils::BinaryIter::new(values_buffer);

    let validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed) => {
                // the pack may contain more items than needed.
                let remaining = length - (offsets.len() - 1);
                let len = std::cmp::min(packed.len() * 8, remaining);
                for is_valid in BitmapIter::new(packed, 0, len) {
                    if is_valid {
                        let value = values_iterator.next().unwrap();
                        last_offset += O::from_usize(value.len()).unwrap();
                        values.extend_from_slice(value);
                    }
                    offsets.push(last_offset);
                }
                validity.extend_from_slice(packed, 0, len);
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
                    offsets.resize(values.len() + additional, last_offset);
                }
            }
        }
    }
}

pub(super) fn read_plain_required<O: Offset>(
    buffer: &[u8],
    additional: usize,
    offsets: &mut Vec<O>,
    values: &mut Vec<u8>,
) {
    let mut last_offset = *offsets.as_mut_slice().last().unwrap();

    let values_iterator = utils::BinaryIter::new(buffer);

    // each value occupies 4 bytes + len declared in 4 bytes => reserve accordingly.
    values.reserve(buffer.len() - 4 * additional);
    let a = values.capacity();
    for value in values_iterator {
        last_offset += O::from_usize(value.len()).unwrap();
        values.extend_from_slice(value);
        offsets.push(last_offset);
    }
    debug_assert_eq!(a, values.capacity());
}

pub(super) fn extend_from_page<O: Offset>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    offsets: &mut Vec<O>,
    values: &mut Vec<u8>,
    validity: &mut MutableBitmap,
) -> Result<()> {
    let additional = page.num_values();
    assert_eq!(descriptor.max_rep_level(), 0);
    assert!(descriptor.max_def_level() <= 1);
    let is_optional = descriptor.max_def_level() == 1;

    let (_, validity_buffer, values_buffer, version) = utils::split_buffer(page, descriptor);

    match (&page.encoding(), page.dictionary_page(), is_optional) {
        (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true) => {
            read_dict_buffer::<O>(
                validity_buffer,
                values_buffer,
                additional,
                dict.as_any().downcast_ref().unwrap(),
                offsets,
                values,
                validity,
            )
        }
        (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false) => {
            read_dict_required::<O>(
                values_buffer,
                additional,
                dict.as_any().downcast_ref().unwrap(),
                offsets,
                values,
                validity,
            )
        }
        (Encoding::DeltaLengthByteArray, None, true) => read_delta_optional::<O>(
            validity_buffer,
            values_buffer,
            additional,
            offsets,
            values,
            validity,
        ),
        (Encoding::Plain, _, true) => read_plain_optional::<O>(
            validity_buffer,
            values_buffer,
            additional,
            offsets,
            values,
            validity,
        ),
        (Encoding::Plain, _, false) => {
            read_plain_required::<O>(page.buffer(), page.num_values(), offsets, values)
        }
        _ => {
            return Err(utils::not_implemented(
                &page.encoding(),
                is_optional,
                page.dictionary_page().is_some(),
                version,
                "Binary",
            ))
        }
    };
    Ok(())
}
