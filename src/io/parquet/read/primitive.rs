use parquet2::{
    encoding::{hybrid_rle, Encoding},
    metadata::ColumnDescriptor,
    read::{decompress_page, Page, PrimitivePageDict},
    serialization::levels,
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

/// Assumptions: No rep levels
fn read_dict_buffer<T: NativeType + ArrowNativeType>(
    validity_buffer: &[u8],
    indices_buffer: &[u8],
    length: u32,
    dict: &PrimitivePageDict<T>,
    _has_validity: bool,
    values: &mut MutableBuffer<T>,
    validity: &mut MutableBitmap,
) {
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
                        dict_values[indices.next().unwrap() as usize]
                    } else {
                        T::default()
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
                        let value = dict_values[index];
                        values.push(value)
                    })
                } else {
                    values.extend_constant(additional, T::default())
                }
            }
        }
    }
}

fn read_buffer<T: NativeType + ArrowNativeType>(
    validity_buffer: &[u8],
    values_buffer: &[u8],
    length: u32,
    _has_validity: bool,
    values: &mut MutableBuffer<T>,
    validity: &mut MutableBitmap,
) {
    let length = length as usize;

    let mut chunks = values_buffer[..length as usize * std::mem::size_of::<T>()]
        .chunks_exact(std::mem::size_of::<T>());

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
                        types::decode(chunks.next().unwrap())
                    } else {
                        T::default()
                    };
                    values.push(value);
                }
            }
            hybrid_rle::HybridEncoded::Rle(value, additional) => {
                let is_set = value[0] == 1;
                validity.extend_constant(additional, is_set);
                if is_set {
                    (0..additional).for_each(|_| {
                        let value = types::decode(chunks.next().unwrap());
                        values.push(value)
                    })
                } else {
                    values.extend_constant(additional, T::default())
                }
            }
        }
    }
}

pub fn iter_to_array<T, I, E>(
    mut iter: I,
    descriptor: &ColumnDescriptor,
    data_type: DataType,
) -> Result<PrimitiveArray<T>>
where
    ArrowError: From<E>,
    T: NativeType + ArrowNativeType,
    I: Iterator<Item = std::result::Result<Page, E>>,
{
    // todo: push metadata from the file to get this capacity
    let capacity = 0;
    let mut values = MutableBuffer::<T>::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);
    iter.try_for_each(|page| extend_from_page(page?, &descriptor, &mut values, &mut validity))?;

    Ok(PrimitiveArray::from_data(
        data_type,
        values.into(),
        validity.into(),
    ))
}

fn extend_from_page<T: NativeType + ArrowNativeType>(
    page: Page,
    descriptor: &ColumnDescriptor,
    values: &mut MutableBuffer<T>,
    validity: &mut MutableBitmap,
) -> Result<()> {
    let page = decompress_page(page)?;
    assert_eq!(descriptor.max_rep_level(), 0);
    assert_eq!(descriptor.max_def_level(), 1);
    let has_validity = descriptor.max_def_level() == 1;
    match page {
        Page::V1(page) => {
            assert_eq!(page.def_level_encoding, Encoding::Rle);
            let (validity_buffer, values_buffer) = utils::split_buffer_v1(&page.buf);

            match (&page.encoding, &page.dictionary_page) {
                (Encoding::PlainDictionary, Some(dict)) => read_dict_buffer::<T>(
                    validity_buffer,
                    values_buffer,
                    page.num_values,
                    dict.as_any().downcast_ref().unwrap(),
                    has_validity,
                    values,
                    validity,
                ),
                (Encoding::Plain, None) => read_buffer::<T>(
                    validity_buffer,
                    values_buffer,
                    page.num_values,
                    has_validity,
                    values,
                    validity,
                ),
                (encoding, None) => {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "Encoding {:?} not yet implemented",
                        encoding
                    )))
                }
                _ => todo!(),
            }
        }
        Page::V2(page) => {
            let def_level_buffer_length = page.header.definition_levels_byte_length as usize;
            let (validity_buffer, values_buffer) =
                utils::split_buffer_v2(&page.buf, def_level_buffer_length);
            match (&page.header.encoding, &page.dictionary_page) {
                (Encoding::PlainDictionary, Some(dict)) => read_dict_buffer::<T>(
                    validity_buffer,
                    values_buffer,
                    page.header.num_values as u32,
                    dict.as_any().downcast_ref().unwrap(),
                    has_validity,
                    values,
                    validity,
                ),
                (Encoding::Plain, None) => read_buffer::<T>(
                    validity_buffer,
                    values_buffer,
                    page.header.num_values as u32,
                    has_validity,
                    values,
                    validity,
                ),
                (encoding, None) => {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "Encoding {:?} not yet implemented",
                        encoding
                    )))
                }
                _ => todo!(),
            }
        }
    };
    Ok(())
}
