mod utils;

use futures::{pin_mut, Stream, StreamExt};
use parquet2::{
    encoding::{hybrid_rle, Encoding},
    page::{DataPage, FixedLenByteArrayPageDict},
    FallibleStreamingIterator,
};

use self::utils::FixedSizeBinary;

use super::{utils::extend_from_decoder, ColumnChunkMetaData, ColumnDescriptor};
use crate::{
    array::FixedSizeBinaryArray,
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::{ArrowError, Result},
};

use super::utils as a_utils;

#[inline]
fn values_iter<'a>(
    indices_buffer: &'a [u8],
    dict_values: &'a [u8],
    size: usize,
    additional: usize,
) -> impl Iterator<Item = &'a [u8]> + 'a {
    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let indices = hybrid_rle::HybridRleDecoder::new(indices_buffer, bit_width as u32, additional);
    indices.map(move |index| {
        let index = index as usize;
        &dict_values[index * size..(index + 1) * size]
    })
}

/// Assumptions: No rep levels
#[allow(clippy::too_many_arguments)]
pub(crate) fn read_dict_buffer(
    validity_buffer: &[u8],
    indices_buffer: &[u8],
    additional: usize,
    dict: &FixedLenByteArrayPageDict,
    values: &mut FixedSizeBinary,
    validity: &mut MutableBitmap,
) {
    let values_iterator = values_iter(indices_buffer, dict.values(), values.size, additional);

    let mut validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    extend_from_decoder(
        validity,
        &mut validity_iterator,
        additional,
        values,
        values_iterator,
    )
}

/// Assumptions: No rep levels
pub(crate) fn read_dict_required(
    indices_buffer: &[u8],
    additional: usize,
    dict: &FixedLenByteArrayPageDict,
    values: &mut FixedSizeBinary,
    validity: &mut MutableBitmap,
) {
    let size = values.size;

    let values_iter = values_iter(indices_buffer, dict.values(), values.size, additional);

    for value in values_iter {
        values.push(value);
    }
    validity.extend_constant(additional * size, true);
}

pub(crate) fn read_optional(
    validity_buffer: &[u8],
    values_buffer: &[u8],
    additional: usize,
    values: &mut FixedSizeBinary,
    validity: &mut MutableBitmap,
) {
    assert_eq!(values_buffer.len() % values.size, 0);
    let values_iterator = values_buffer.chunks_exact(values.size);

    let mut validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    extend_from_decoder(
        validity,
        &mut validity_iterator,
        additional,
        values,
        values_iterator,
    )
}

pub(crate) fn read_required(buffer: &[u8], additional: usize, values: &mut FixedSizeBinary) {
    assert_eq!(buffer.len(), additional * values.size);
    values.values.extend_from_slice(buffer);
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
    let size = FixedSizeBinaryArray::get_size(&data_type);

    let capacity = metadata.num_values() as usize;
    let mut values = FixedSizeBinary::with_capacity(capacity, size);
    let mut validity = MutableBitmap::with_capacity(capacity);
    while let Some(page) = iter.next()? {
        extend_from_page(page, metadata.descriptor(), &mut values, &mut validity)?
    }

    Ok(FixedSizeBinaryArray::from_data(
        data_type,
        values.values.into(),
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
    let size = FixedSizeBinaryArray::get_size(&data_type);

    let capacity = metadata.num_values() as usize;
    let mut values = FixedSizeBinary::with_capacity(capacity, size);
    let mut validity = MutableBitmap::with_capacity(capacity);

    pin_mut!(pages); // needed for iteration

    while let Some(page) = pages.next().await {
        extend_from_page(
            page.as_ref().map_err(|x| x.clone())?,
            metadata.descriptor(),
            &mut values,
            &mut validity,
        )?
    }

    Ok(FixedSizeBinaryArray::from_data(
        data_type,
        values.values.into(),
        validity.into(),
    ))
}

pub(crate) fn extend_from_page(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    values: &mut FixedSizeBinary,
    validity: &mut MutableBitmap,
) -> Result<()> {
    let additional = page.num_values();
    assert_eq!(descriptor.max_rep_level(), 0);
    assert!(descriptor.max_def_level() <= 1);
    let is_optional = descriptor.max_def_level() == 1;

    let (_, validity_buffer, values_buffer, version) = a_utils::split_buffer(page, descriptor);

    match (page.encoding(), page.dictionary_page(), is_optional) {
        (Encoding::PlainDictionary, Some(dict), true) => read_dict_buffer(
            validity_buffer,
            values_buffer,
            additional,
            dict.as_any().downcast_ref().unwrap(),
            values,
            validity,
        ),
        (Encoding::PlainDictionary, Some(dict), false) => read_dict_required(
            values_buffer,
            additional,
            dict.as_any().downcast_ref().unwrap(),
            values,
            validity,
        ),
        (Encoding::Plain, _, true) => {
            read_optional(validity_buffer, values_buffer, additional, values, validity)
        }
        // it can happen that there is a dictionary but the encoding is plain because
        // it falled back.
        (Encoding::Plain, _, false) => read_required(page.buffer(), additional, values),
        _ => {
            return Err(a_utils::not_implemented(
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
