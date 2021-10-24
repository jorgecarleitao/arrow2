use futures::{pin_mut, Stream, StreamExt};
use parquet2::{
    encoding::{hybrid_rle, Encoding},
    page::{DataPage, FixedLenByteArrayPageDict},
    FallibleStreamingIterator,
};

use super::{ColumnChunkMetaData, ColumnDescriptor};
use crate::{
    array::FixedSizeBinaryArray,
    bitmap::{utils::BitmapIter, MutableBitmap},
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
    additional: usize,
    size: usize,
    dict: &FixedLenByteArrayPageDict,
    values: &mut MutableBuffer<u8>,
    validity: &mut MutableBitmap,
) {
    let length = values.len() + additional * size;
    let dict_values = dict.values();

    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let mut indices = hybrid_rle::HybridRleDecoder::new(indices_buffer, bit_width as u32, length);

    let validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed) => {
                let remaining = (length - values.len()) / size;
                let len = std::cmp::min(packed.len() * 8, remaining);
                for is_valid in BitmapIter::new(packed, 0, len) {
                    validity.push(is_valid);
                    if is_valid {
                        let index = indices.next().unwrap() as usize;
                        values.extend_from_slice(&dict_values[index * size..(index + 1) * size]);
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
                        values.extend_from_slice(&dict_values[index * size..(index + 1) * size]);
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
    additional: usize,
    size: usize,
    values: &mut MutableBuffer<u8>,
    validity: &mut MutableBitmap,
) {
    let length = values.len() + additional * size;

    assert_eq!(values_buffer.len() % size, 0);
    let mut values_iterator = values_buffer.chunks_exact(size);

    let validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed) => {
                // the pack may contain more items than needed.
                let remaining = (length - values.len()) / size;
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

pub(crate) fn read_required(
    buffer: &[u8],
    additional: usize,
    size: usize,
    values: &mut MutableBuffer<u8>,
) {
    assert_eq!(buffer.len(), additional * size);
    values.extend_from_slice(buffer);
}

pub fn iter_to_array<I, E>(
    mut iter: I,
    data_type: DataType,
    metadata: &ColumnChunkMetaData,
) -> Result<FixedSizeBinaryArray>
where
    ArrowError: From<E>,
    I: FallibleStreamingIterator<Item = DataPage, Error = E>,
{
    let size = *FixedSizeBinaryArray::get_size(&data_type) as usize;

    let capacity = metadata.num_values() as usize;
    let mut values = MutableBuffer::<u8>::with_capacity(capacity * size);
    let mut validity = MutableBitmap::with_capacity(capacity);
    while let Some(page) = iter.next()? {
        extend_from_page(
            page,
            size,
            metadata.descriptor(),
            &mut values,
            &mut validity,
        )?
    }

    Ok(FixedSizeBinaryArray::from_data(
        data_type,
        values.into(),
        validity.into(),
    ))
}

pub async fn stream_to_array<I, E>(
    pages: I,
    data_type: DataType,
    metadata: &ColumnChunkMetaData,
) -> Result<FixedSizeBinaryArray>
where
    ArrowError: From<E>,
    E: Clone,
    I: Stream<Item = std::result::Result<DataPage, E>>,
{
    let size = *FixedSizeBinaryArray::get_size(&data_type) as usize;

    let capacity = metadata.num_values() as usize;
    let mut values = MutableBuffer::<u8>::with_capacity(capacity * size);
    let mut validity = MutableBitmap::with_capacity(capacity);

    pin_mut!(pages); // needed for iteration

    while let Some(page) = pages.next().await {
        extend_from_page(
            page.as_ref().map_err(|x| x.clone())?,
            size,
            metadata.descriptor(),
            &mut values,
            &mut validity,
        )?
    }

    Ok(FixedSizeBinaryArray::from_data(
        data_type,
        values.into(),
        validity.into(),
    ))
}

pub(crate) fn extend_from_page(
    page: &DataPage,
    size: usize,
    descriptor: &ColumnDescriptor,
    values: &mut MutableBuffer<u8>,
    validity: &mut MutableBitmap,
) -> Result<()> {
    let additional = page.num_values();
    assert_eq!(descriptor.max_rep_level(), 0);
    assert!(descriptor.max_def_level() <= 1);
    let is_optional = descriptor.max_def_level() == 1;

    let (_, validity_buffer, values_buffer, version) = utils::split_buffer(page, descriptor);

    match (page.encoding(), page.dictionary_page(), is_optional) {
        (Encoding::PlainDictionary, Some(dict), true) => read_dict_buffer(
            validity_buffer,
            values_buffer,
            additional,
            size,
            dict.as_any().downcast_ref().unwrap(),
            values,
            validity,
        ),
        (Encoding::Plain, _, true) => read_optional(
            validity_buffer,
            values_buffer,
            additional,
            size,
            values,
            validity,
        ),
        // it can happen that there is a dictionary but the encoding is plain because
        // it falled back.
        (Encoding::Plain, _, false) => read_required(page.buffer(), additional, size, values),
        _ => {
            return Err(utils::not_implemented(
                &page.encoding(),
                is_optional,
                page.dictionary_page().is_some(),
                version,
                "FixedSizeBinary",
            ))
        }
    }
    Ok(())
}
