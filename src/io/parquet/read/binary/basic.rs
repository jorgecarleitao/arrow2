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

use super::{super::utils, utils::Binary};

/// Assumptions: No rep levels
#[allow(clippy::too_many_arguments)]
fn read_dict_buffer<O: Offset>(
    validity_buffer: &[u8],
    indices_buffer: &[u8],
    additional: usize,
    dict: &BinaryPageDict,
    values: &mut Binary<O>,
    validity: &mut MutableBitmap,
) {
    let length = values.len() + additional;
    let dict_values = dict.values();
    let dict_offsets = dict.offsets();

    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let mut indices = hybrid_rle::HybridRleDecoder::new(indices_buffer, bit_width as u32, length);

    let validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed) => {
                let remaining = length - values.len();
                let len = std::cmp::min(packed.len() * 8, remaining);
                for is_valid in BitmapIter::new(packed, 0, len) {
                    if is_valid {
                        let index = indices.next().unwrap() as usize;
                        let dict_offset_i = dict_offsets[index] as usize;
                        let dict_offset_ip1 = dict_offsets[index + 1] as usize;
                        values.push(&dict_values[dict_offset_i..dict_offset_ip1]);
                    } else {
                        values.push(&[])
                    }
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
                        values.push(&dict_values[dict_offset_i..dict_offset_ip1]);
                    })
                } else {
                    values.extend_constant(additional)
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
    values: &mut Binary<O>,
    validity: &mut MutableBitmap,
) {
    let dict_values = dict.values();
    let dict_offsets = dict.offsets();

    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let indices = hybrid_rle::HybridRleDecoder::new(indices_buffer, bit_width as u32, additional);

    for index in indices {
        let index = index as usize;
        let dict_offset_i = dict_offsets[index] as usize;
        let dict_offset_ip1 = dict_offsets[index + 1] as usize;
        values.push(&dict_values[dict_offset_i..dict_offset_ip1]);
    }
    validity.extend_constant(additional, true);
}

fn read_delta_optional<O: Offset>(
    validity_buffer: &[u8],
    values_buffer: &[u8],
    additional: usize,
    values: &mut Binary<O>,
    validity: &mut MutableBitmap,
) {
    let Binary {
        offsets,
        values,
        last_offset,
    } = values;

    let length = (offsets.len() - 1) + additional;

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
                        *last_offset += O::from_usize(value).unwrap();
                    }
                    offsets.push(*last_offset);
                }
                validity.extend_from_slice(packed, 0, len);
            }
            hybrid_rle::HybridEncoded::Rle(value, additional) => {
                let is_set = value[0] == 1;
                validity.extend_constant(additional, is_set);
                if is_set {
                    (0..additional).for_each(|_| {
                        let value = values_iterator.next().unwrap() as usize;
                        *last_offset += O::from_usize(value).unwrap();
                        offsets.push(*last_offset);
                    })
                } else {
                    offsets.resize(offsets.len() + additional, *last_offset);
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
    values: &mut Binary<O>,
    validity: &mut MutableBitmap,
) {
    let length = values.len() + additional;

    // values_buffer: first 4 bytes are len, remaining is values
    let mut values_iterator = utils::BinaryIter::new(values_buffer);

    let validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed) => {
                // the pack may contain more items than needed.
                let remaining = length - values.len();
                let len = std::cmp::min(packed.len() * 8, remaining);
                for is_valid in BitmapIter::new(packed, 0, len) {
                    if is_valid {
                        values.push(values_iterator.next().unwrap());
                    } else {
                        values.push(&[]);
                    }
                }
                validity.extend_from_slice(packed, 0, len);
            }
            hybrid_rle::HybridEncoded::Rle(value, additional) => {
                let is_set = value[0] == 1;
                validity.extend_constant(additional, is_set);
                if is_set {
                    (0..additional).for_each(|_| {
                        let value = values_iterator.next().unwrap();
                        values.push(value);
                    })
                } else {
                    values.extend_constant(additional);
                }
            }
        }
    }
}

pub(super) fn read_plain_required<O: Offset>(
    buffer: &[u8],
    additional: usize,
    values: &mut Binary<O>,
) {
    let values_iterator = utils::BinaryIter::new(buffer);

    // each value occupies 4 bytes + len declared in 4 bytes => reserve accordingly.
    values.offsets.reserve(additional);
    values.values.reserve(buffer.len() - 4 * additional);
    let a = values.values.capacity();
    for value in values_iterator {
        values.push(value);
    }
    debug_assert_eq!(a, values.values.capacity());
}

pub(super) fn extend_from_page<O: Offset>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    values: &mut Binary<O>,
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
                values,
                validity,
            )
        }
        (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false) => {
            read_dict_required::<O>(
                values_buffer,
                additional,
                dict.as_any().downcast_ref().unwrap(),
                values,
                validity,
            )
        }
        (Encoding::DeltaLengthByteArray, None, true) => {
            read_delta_optional::<O>(validity_buffer, values_buffer, additional, values, validity)
        }
        (Encoding::Plain, _, true) => {
            read_plain_optional::<O>(validity_buffer, values_buffer, additional, values, validity)
        }
        (Encoding::Plain, _, false) => {
            read_plain_required::<O>(page.buffer(), page.num_values(), values)
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
