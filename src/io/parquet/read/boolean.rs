use crate::{
    array::BooleanArray,
    bitmap::{BitmapIter, MutableBitmap},
    error::{ArrowError, Result},
};

use super::utils;
use parquet2::{
    encoding::{hybrid_rle, Encoding},
    metadata::ColumnDescriptor,
    read::{decompress_page, Page},
};

fn read_bitmap(
    validity_buffer: &[u8],
    values_buffer: &[u8],
    length: u32,
    _has_validity: bool,
    values: &mut MutableBitmap,
    validity: &mut MutableBitmap,
) {
    let length = length as usize;

    let validity_iterator = hybrid_rle::Decoder::new(&validity_buffer, 1);

    // in PLAIN, booleans are LSB bitpacked and thus we can read them as if they were a bitmap.
    // note that `values_buffer` contains only non-null values.
    // thus, at this point, it is not known how many values this buffer contains
    // values_len is the upper bound. The actual number depends on how many nulls there is.
    let values_len = values_buffer.len() * 8;
    let mut values_iterator = BitmapIter::new(values_buffer, 0, values_len);

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
                        values_iterator.next().unwrap()
                    } else {
                        false
                    };
                    values.push(value);
                }
            }
            hybrid_rle::HybridEncoded::Rle(value, additional) => {
                let is_set = value[0] == 1;
                validity.extend_constant(additional, is_set);
                if is_set {
                    (0..additional).for_each(|_| {
                        let value = values_iterator.next().unwrap();
                        values.push(value)
                    })
                } else {
                    values.extend_constant(additional, false)
                }
            }
        }
    }
}

pub fn iter_to_array<I, E>(mut iter: I, descriptor: &ColumnDescriptor) -> Result<BooleanArray>
where
    ArrowError: From<E>,
    I: Iterator<Item = std::result::Result<Page, E>>,
{
    // todo: push metadata from the file to get this capacity
    let capacity = 0;
    let mut values = MutableBitmap::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);
    iter.try_for_each(|page| extend_from_page(page?, &descriptor, &mut values, &mut validity))?;

    Ok(BooleanArray::from_data(values.into(), validity.into()))
}

fn extend_from_page(
    page: Page,
    descriptor: &ColumnDescriptor,
    values: &mut MutableBitmap,
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
                (Encoding::Plain, None) => read_bitmap(
                    validity_buffer,
                    values_buffer,
                    page.num_values,
                    has_validity,
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
        Page::V2(page) => {
            let def_level_buffer_length = page.header.definition_levels_byte_length as usize;
            let (validity_buffer, values_buffer) =
                utils::split_buffer_v2(&page.buf, def_level_buffer_length);
            match (&page.header.encoding, &page.dictionary_page) {
                (Encoding::Plain, None) => read_bitmap(
                    validity_buffer,
                    values_buffer,
                    page.header.num_values as u32,
                    has_validity,
                    values,
                    validity,
                ),
                (encoding, None) => {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "Encoding {:?} not yet implemented for boolean values",
                        encoding
                    )))
                }
                _ => todo!(),
            }
        }
    };
    Ok(())
}
