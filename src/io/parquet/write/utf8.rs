use parquet2::{
    compression::create_codec,
    encoding::Encoding,
    metadata::ColumnDescriptor,
    read::{CompressedPage, PageHeader},
    schema::DataPageHeader,
    statistics::{serialize_statistics, BinaryStatistics, ParquetStatistics, Statistics},
    write::WriteOptions,
};

use super::binary::ord_binary;
use super::utils;
use crate::{
    array::{Array, Offset, Utf8Array},
    error::Result,
    io::parquet::read::is_type_nullable,
};

pub fn array_to_page_v1<O: Offset>(
    array: &Utf8Array<O>,
    options: WriteOptions,
    descriptor: ColumnDescriptor,
) -> Result<CompressedPage> {
    let is_optional = is_type_nullable(descriptor.type_());

    let validity = array.validity();

    let mut buffer = utils::write_def_levels(is_optional, validity, array.len())?;

    // append the non-null values
    if is_optional {
        array.iter().for_each(|x| {
            if let Some(x) = x {
                // BYTE_ARRAY: first 4 bytes denote length in littleendian.
                let len = (x.len() as u32).to_le_bytes();
                buffer.extend_from_slice(&len);
                buffer.extend_from_slice(x.as_bytes());
            }
        })
    } else {
        array.values_iter().for_each(|x| {
            // BYTE_ARRAY: first 4 bytes denote length in littleendian.
            let len = (x.len() as u32).to_le_bytes();
            buffer.extend_from_slice(&len);
            buffer.extend_from_slice(x.as_bytes());
        })
    }
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
        Some(build_statistics(array, descriptor.clone()))
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

fn build_statistics<O: Offset>(
    array: &Utf8Array<O>,
    descriptor: ColumnDescriptor,
) -> ParquetStatistics {
    let statistics = &BinaryStatistics {
        descriptor,
        null_count: Some(array.null_count() as i64),
        distinct_count: None,
        max_value: array
            .iter()
            .flatten()
            .map(|x| x.as_bytes())
            .max_by(|x, y| ord_binary(x, y))
            .map(|x| x.to_vec()),
        min_value: array
            .iter()
            .flatten()
            .map(|x| x.as_bytes())
            .min_by(|x, y| ord_binary(x, y))
            .map(|x| x.to_vec()),
    } as &dyn Statistics;
    serialize_statistics(statistics)
}
