use parquet2::{
    compression::create_codec,
    encoding::Encoding,
    metadata::ColumnDescriptor,
    read::{CompressedPage, PageHeader},
    schema::DataPageHeader,
    statistics::{serialize_statistics, ParquetStatistics, PrimitiveStatistics, Statistics},
    types::NativeType,
    write::WriteOptions,
};

use super::utils;
use crate::{
    array::{Array, PrimitiveArray},
    error::Result,
    io::parquet::read::is_type_nullable,
    types::NativeType as ArrowNativeType,
};

pub fn array_to_page_v1<T, R>(
    array: &PrimitiveArray<T>,
    options: WriteOptions,
    descriptor: ColumnDescriptor,
) -> Result<CompressedPage>
where
    T: ArrowNativeType,
    R: NativeType,
    T: num::cast::AsPrimitive<R>,
{
    let is_optional = is_type_nullable(descriptor.type_());

    let validity = array.validity();

    let mut buffer = utils::write_def_levels(is_optional, validity, array.len())?;

    if is_optional {
        // append the non-null values
        array.iter().for_each(|x| {
            if let Some(x) = x {
                let parquet_native: R = x.as_();
                buffer.extend_from_slice(parquet_native.to_le_bytes().as_ref())
            }
        });
    } else {
        // append all values
        array.values().iter().for_each(|x| {
            let parquet_native: R = x.as_();
            buffer.extend_from_slice(parquet_native.to_le_bytes().as_ref())
        });
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

fn build_statistics<T, R>(array: &PrimitiveArray<T>) -> ParquetStatistics
where
    T: ArrowNativeType,
    R: NativeType,
    T: num::cast::AsPrimitive<R>,
{
    let statistics = &PrimitiveStatistics::<R> {
        null_count: Some(array.null_count() as i64),
        distinct_count: None,
        max_value: array
            .iter()
            .flatten()
            .map(|x| {
                let x: R = x.as_();
                x
            })
            .max_by(|x, y| x.ord(y)),
        min_value: array
            .iter()
            .flatten()
            .map(|x| {
                let x: R = x.as_();
                x
            })
            .min_by(|x, y| x.ord(y)),
    } as &dyn Statistics;
    serialize_statistics(statistics)
}
