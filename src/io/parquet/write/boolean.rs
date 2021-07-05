use parquet2::{
    encoding::hybrid_rle::bitpacked_encode,
    metadata::ColumnDescriptor,
    read::CompressedPage,
    statistics::{serialize_statistics, BooleanStatistics, ParquetStatistics, Statistics},
    write::WriteOptions,
};

use super::utils;
use crate::error::Result;
use crate::{array::*, io::parquet::read::is_type_nullable};

#[inline]
fn encode(iterator: impl Iterator<Item = bool>, buffer: Vec<u8>) -> Result<Vec<u8>> {
    // encode values using bitpacking
    let len = buffer.len();
    let mut buffer = std::io::Cursor::new(buffer);
    buffer.set_position(len as u64);
    bitpacked_encode(&mut buffer, iterator)?;
    Ok(buffer.into_inner())
}

pub fn array_to_page(
    array: &BooleanArray,
    options: WriteOptions,
    descriptor: ColumnDescriptor,
) -> Result<CompressedPage> {
    let is_optional = is_type_nullable(descriptor.type_());

    let validity = array.validity();

    let buffer = utils::write_def_levels(is_optional, validity, array.len(), options.version)?;

    let definition_levels_byte_length = buffer.len();

    let buffer = if is_optional {
        let iter = array.iter().flatten().take(
            validity
                .as_ref()
                .map(|x| x.len() - x.null_count())
                .unwrap_or_else(|| array.len()),
        );
        encode(iter, buffer)
    } else {
        let iter = array.values().iter();
        encode(iter, buffer)
    }?;

    let uncompressed_page_size = buffer.len();

    let buffer = utils::compress(buffer, options, definition_levels_byte_length)?;

    let statistics = if options.write_statistics {
        Some(build_statistics(array))
    } else {
        None
    };

    utils::build_plain_page(
        buffer,
        array.len(),
        array.null_count(),
        uncompressed_page_size,
        definition_levels_byte_length,
        statistics,
        descriptor,
        options,
    )
}

fn build_statistics(array: &BooleanArray) -> ParquetStatistics {
    let statistics = &BooleanStatistics {
        null_count: Some(array.null_count() as i64),
        distinct_count: None,
        max_value: array.iter().flatten().max(),
        min_value: array.iter().flatten().min(),
    } as &dyn Statistics;
    serialize_statistics(statistics)
}
