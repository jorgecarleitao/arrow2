use parquet2::{
    metadata::ColumnDescriptor,
    page::CompressedDataPage,
    schema::Encoding,
    statistics::{serialize_statistics, ParquetStatistics, PrimitiveStatistics, Statistics},
    types::NativeType,
    write::WriteOptions,
};

use super::super::utils;
use crate::{
    array::{Array, PrimitiveArray},
    error::Result,
    io::parquet::read::is_type_nullable,
    types::NativeType as ArrowNativeType,
};

pub(crate) fn encode_plain<T, R>(array: &PrimitiveArray<T>, is_optional: bool, buffer: &mut Vec<u8>)
where
    T: ArrowNativeType,
    R: NativeType,
    T: num::cast::AsPrimitive<R>,
{
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
}

pub fn array_to_page<T, R>(
    array: &PrimitiveArray<T>,
    options: WriteOptions,
    descriptor: ColumnDescriptor,
) -> Result<CompressedDataPage>
where
    T: ArrowNativeType,
    R: NativeType,
    T: num::cast::AsPrimitive<R>,
{
    let is_optional = is_type_nullable(descriptor.type_());

    let validity = array.validity();

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
        Encoding::Plain,
    )
}

pub fn build_statistics<T, R>(
    array: &PrimitiveArray<T>,
    descriptor: ColumnDescriptor,
) -> ParquetStatistics
where
    T: ArrowNativeType,
    R: NativeType,
    T: num::cast::AsPrimitive<R>,
{
    let statistics = &PrimitiveStatistics::<R> {
        descriptor,
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
