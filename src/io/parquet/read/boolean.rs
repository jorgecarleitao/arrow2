use crate::{
    array::BooleanArray,
    bitmap::{BitmapIter, MutableBitmap},
    error::{ArrowError, Result},
};

use super::utils;
use parquet2::{
    encoding::{hybrid_rle, Encoding},
    metadata::{ColumnChunkMetaData, ColumnDescriptor},
    read::{Page, PageHeader, StreamingIterator},
};

fn read_required(buffer: &[u8], length: u32, values: &mut MutableBitmap) {
    let length = length as usize;

    // in PLAIN, booleans are LSB bitpacked and thus we can read them as if they were a bitmap.
    // note that `values_buffer` contains only non-null values.
    let values_iterator = BitmapIter::new(buffer, 0, length);

    values.extend_from_trusted_len_iter(values_iterator);
}

fn read_optional(
    validity_buffer: &[u8],
    values_buffer: &[u8],
    length: u32,
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

    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed_validity) => {
                // the pack may contain more items than needed.
                let remaining = length - values.len();
                let len = std::cmp::min(packed_validity.len() * 8, remaining);
                for is_valid in BitmapIter::new(packed_validity, 0, len) {
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

pub fn iter_to_array<I, E>(mut iter: I, metadata: &ColumnChunkMetaData) -> Result<BooleanArray>
where
    ArrowError: From<E>,
    E: Clone,
    I: StreamingIterator<Item = std::result::Result<Page, E>>,
{
    let capacity = metadata.num_values() as usize;
    let mut values = MutableBitmap::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);
    while let Some(page) = iter.next() {
        extend_from_page(
            page.as_ref().map_err(|x| x.clone())?,
            metadata.descriptor(),
            &mut values,
            &mut validity,
        )?
    }

    Ok(BooleanArray::from_data(values.into(), validity.into()))
}

fn extend_from_page(
    page: &Page,
    descriptor: &ColumnDescriptor,
    values: &mut MutableBitmap,
    validity: &mut MutableBitmap,
) -> Result<()> {
    assert_eq!(descriptor.max_rep_level(), 0);
    assert!(descriptor.max_def_level() <= 1);
    let is_optional = descriptor.max_def_level() == 1;
    match page.header() {
        PageHeader::V1(header) => {
            assert_eq!(header.definition_level_encoding, Encoding::Rle);

            match (&page.encoding(), page.dictionary_page(), is_optional) {
                (Encoding::Plain, None, true) => {
                    let (validity_buffer, values_buffer) = utils::split_buffer_v1(page.buffer());
                    read_optional(
                        validity_buffer,
                        values_buffer,
                        page.num_values() as u32,
                        values,
                        validity,
                    )
                }
                (Encoding::Plain, None, false) => {
                    read_required(page.buffer(), page.num_values() as u32, values)
                }
                _ => {
                    return Err(utils::not_implemented(
                        &page.encoding(),
                        is_optional,
                        page.dictionary_page().is_some(),
                        "V1",
                        "Boolean",
                    ))
                }
            }
        }
        PageHeader::V2(header) => {
            let def_level_buffer_length = header.definition_levels_byte_length as usize;
            match (page.encoding(), page.dictionary_page(), is_optional) {
                (Encoding::Plain, None, true) => {
                    let (validity_buffer, values_buffer) =
                        utils::split_buffer_v2(page.buffer(), def_level_buffer_length);
                    read_optional(
                        validity_buffer,
                        values_buffer,
                        page.num_values() as u32,
                        values,
                        validity,
                    )
                }
                (Encoding::Plain, None, false) => {
                    read_required(page.buffer(), page.num_values() as u32, values)
                }
                _ => {
                    return Err(utils::not_implemented(
                        &page.encoding(),
                        is_optional,
                        page.dictionary_page().is_some(),
                        "V2",
                        "Boolean",
                    ))
                }
            }
        }
    };
    Ok(())
}
