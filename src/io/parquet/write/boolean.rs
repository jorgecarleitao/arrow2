use parquet2::{
    compression::create_codec,
    encoding::{hybrid_rle::bitpacked_encode, Encoding},
    metadata::ColumnDescriptor,
    read::{CompressedPage, PageHeader},
    schema::DataPageHeader,
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

pub fn array_to_page_v1(
    array: &BooleanArray,
    options: WriteOptions,
    descriptor: ColumnDescriptor,
) -> Result<CompressedPage> {
    let is_optional = is_type_nullable(descriptor.type_());

    let validity = array.validity();

    let buffer = utils::write_def_levels(is_optional, validity, array.len())?;

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

    let codec = create_codec(&options.compression)?;
    let buffer = if let Some(mut codec) = codec {
        // todo: remove this allocation by extending `buffer` directly.
        // needs refactoring `compress`'s API.
        let mut tmp = vec![];
        codec.compress(&buffer, &mut tmp)?;
        tmp
    } else {
        buffer
    };

    let statistics = if options.write_statistics {
        Some(build_statistics(array))
    } else {
        None
    };

    let header = PageHeader::V1(DataPageHeader {
        num_values: array.len() as i32,
        encoding: Encoding::Plain,
        definition_level_encoding: Encoding::Rle,
        repetition_level_encoding: Encoding::Rle,
        statistics,
    });

    Ok(CompressedPage::new(
        header,
        buffer,
        options.compression,
        uncompressed_page_size,
        None,
        descriptor,
    ))
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
