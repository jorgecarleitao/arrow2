use crate::{
    array::BooleanArray,
    bitmap::{utils::BitmapIter, MutableBitmap},
    datatypes::DataType,
    error::{ArrowError, Result},
    io::parquet::read::utils::extend_from_decoder,
};

use super::super::utils;

use futures::{pin_mut, Stream, StreamExt};
use parquet2::{
    encoding::{hybrid_rle, Encoding},
    metadata::{ColumnChunkMetaData, ColumnDescriptor},
    page::DataPage,
};

pub(super) fn read_required(buffer: &[u8], additional: usize, values: &mut MutableBitmap) {
    // in PLAIN, booleans are LSB bitpacked and thus we can read them as if they were a bitmap.
    values.extend_from_slice(buffer, 0, additional);
}

fn read_optional(
    validity_buffer: &[u8],
    values_buffer: &[u8],
    length: usize,
    values: &mut MutableBitmap,
    validity: &mut MutableBitmap,
) {
    // in PLAIN, booleans are LSB bitpacked and thus we can read them as if they were a bitmap.
    // note that `values_buffer` contains only non-null values.
    // thus, at this point, it is not known how many values this buffer contains
    // values_len is the upper bound. The actual number depends on how many nulls there is.
    let values_len = values_buffer.len() * 8;
    let values_iterator = BitmapIter::new(values_buffer, 0, values_len);

    let mut validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    extend_from_decoder(
        validity,
        &mut validity_iterator,
        length,
        values,
        values_iterator,
    )
}

pub async fn stream_to_array<I, E>(pages: I, metadata: &ColumnChunkMetaData) -> Result<BooleanArray>
where
    ArrowError: From<E>,
    E: Clone,
    I: Stream<Item = std::result::Result<DataPage, E>>,
{
    let capacity = metadata.num_values() as usize;
    let mut values = MutableBitmap::with_capacity(capacity);
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

    Ok(BooleanArray::from_data(
        DataType::Boolean,
        values.into(),
        validity.into(),
    ))
}

pub(super) fn extend_from_page(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    values: &mut MutableBitmap,
    validity: &mut MutableBitmap,
) -> Result<()> {
    assert_eq!(descriptor.max_rep_level(), 0);
    assert!(descriptor.max_def_level() <= 1);
    let is_optional = descriptor.max_def_level() == 1;

    let (_, validity_buffer, values_buffer, version) = utils::split_buffer(page, descriptor);

    match (page.encoding(), page.dictionary_page(), is_optional) {
        (Encoding::Plain, None, true) => read_optional(
            validity_buffer,
            values_buffer,
            page.num_values(),
            values,
            validity,
        ),
        (Encoding::Plain, None, false) => read_required(page.buffer(), page.num_values(), values),
        _ => {
            return Err(utils::not_implemented(
                &page.encoding(),
                is_optional,
                page.dictionary_page().is_some(),
                version,
                "Boolean",
            ))
        }
    }
    Ok(())
}
