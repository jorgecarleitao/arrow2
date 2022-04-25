use parquet2::{
    encoding::Encoding,
    metadata::Descriptor,
    page::DataPage,
    schema::types::PrimitiveType,
    statistics::{serialize_statistics, PrimitiveStatistics},
    types::NativeType,
};

use super::super::utils;
use super::super::WriteOptions;
use crate::{
    array::{Array, PrimitiveArray},
    error::Result,
    io::parquet::read::schema::is_nullable,
    types::NativeType as ArrowNativeType,
};

pub(crate) fn encode_plain<T, R>(array: &PrimitiveArray<T>, is_optional: bool, buffer: &mut Vec<u8>)
where
    T: ArrowNativeType,
    R: NativeType,
    T: num_traits::AsPrimitive<R>,
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
    descriptor: Descriptor,
) -> Result<DataPage>
where
    T: ArrowNativeType,
    R: NativeType,
    T: num_traits::AsPrimitive<R>,
{
    let is_optional = is_nullable(&descriptor.primitive_type.field_info);

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

    let statistics = if options.write_statistics {
        Some(serialize_statistics(&build_statistics(
            array,
            descriptor.primitive_type.clone(),
        )))
    } else {
        None
    };

    utils::build_plain_page(
        buffer,
        array.len(),
        array.len(),
        array.null_count(),
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
    primitive_type: PrimitiveType,
) -> PrimitiveStatistics<R>
where
    T: ArrowNativeType,
    R: NativeType,
    T: num_traits::AsPrimitive<R>,
{
    PrimitiveStatistics::<R> {
        primitive_type,
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
    }
}
