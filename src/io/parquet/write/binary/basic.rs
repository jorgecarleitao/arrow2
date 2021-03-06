use parquet2::{
    metadata::ColumnDescriptor,
    read::CompressedPage,
    statistics::{serialize_statistics, BinaryStatistics, ParquetStatistics, Statistics},
    write::WriteOptions,
};

use super::super::utils;
use crate::{
    array::{Array, BinaryArray, Offset},
    error::Result,
    io::parquet::read::is_type_nullable,
};

pub(super) fn encode_plain<O: Offset>(
    array: &BinaryArray<O>,
    is_optional: bool,
    buffer: &mut Vec<u8>,
) {
    // append the non-null values
    if is_optional {
        array.iter().for_each(|x| {
            if let Some(x) = x {
                // BYTE_ARRAY: first 4 bytes denote length in littleendian.
                let len = (x.len() as u32).to_le_bytes();
                buffer.extend_from_slice(&len);
                buffer.extend_from_slice(x);
            }
        })
    } else {
        array.values_iter().for_each(|x| {
            // BYTE_ARRAY: first 4 bytes denote length in littleendian.
            let len = (x.len() as u32).to_le_bytes();
            buffer.extend_from_slice(&len);
            buffer.extend_from_slice(x);
        })
    }
}

pub fn array_to_page<O: Offset>(
    array: &BinaryArray<O>,
    options: WriteOptions,
    descriptor: ColumnDescriptor,
) -> Result<CompressedPage> {
    let validity = array.validity();
    let is_optional = is_type_nullable(descriptor.type_());

    let mut buffer = vec![];
    utils::write_def_levels(
        &mut buffer,
        is_optional,
        validity,
        array.len(),
        options.version,
    )?;

    let definition_levels_byte_length = buffer.len();

    encode_plain(array, is_optional, &mut buffer);

    let uncompressed_page_size = buffer.len();

    let buffer = utils::compress(buffer, options, definition_levels_byte_length)?;

    let statistics = if options.write_statistics {
        Some(build_statistics(array, descriptor.clone()))
    } else {
        None
    };

    utils::build_plain_page(
        buffer,
        array.len(),
        array.null_count(),
        uncompressed_page_size,
        0,
        definition_levels_byte_length,
        statistics,
        descriptor,
        options,
    )
}

pub(super) fn build_statistics<O: Offset>(
    array: &BinaryArray<O>,
    descriptor: ColumnDescriptor,
) -> ParquetStatistics {
    let statistics = &BinaryStatistics {
        descriptor,
        null_count: Some(array.null_count() as i64),
        distinct_count: None,
        max_value: array
            .iter()
            .flatten()
            .max_by(|x, y| ord_binary(x, y))
            .map(|x| x.to_vec()),
        min_value: array
            .iter()
            .flatten()
            .min_by(|x, y| ord_binary(x, y))
            .map(|x| x.to_vec()),
    } as &dyn Statistics;
    serialize_statistics(statistics)
}

/// Returns the ordering of two binary values. This corresponds to pyarrows' ordering
/// of statistics.
pub(crate) fn ord_binary<'a>(a: &'a [u8], b: &'a [u8]) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a.is_empty(), b.is_empty()) {
        (true, true) => return Equal,
        (true, false) => return Less,
        (false, true) => return Greater,
        (false, false) => {}
    }

    for (v1, v2) in a.iter().zip(b.iter()) {
        match v1.cmp(v2) {
            Equal => continue,
            other => return other,
        }
    }
    Equal
}
