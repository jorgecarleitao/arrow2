use parquet2::{
    compression::create_codec, metadata::ColumnDescriptor, page::CompressedDataPage,
    schema::Encoding, write::WriteOptions,
};

use super::utils;
use crate::{
    array::{Array, FixedSizeBinaryArray},
    error::Result,
    io::parquet::read::is_type_nullable,
};

pub fn array_to_page(
    array: &FixedSizeBinaryArray,
    options: WriteOptions,
    descriptor: ColumnDescriptor,
) -> Result<CompressedDataPage> {
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

    if is_optional {
        // append the non-null values
        array.iter().for_each(|x| {
            if let Some(x) = x {
                buffer.extend_from_slice(x);
            }
        });
    } else {
        // append all values
        buffer.extend_from_slice(array.values());
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

    utils::build_plain_page(
        buffer,
        array.len(),
        array.null_count(),
        uncompressed_page_size,
        0,
        definition_levels_byte_length,
        None,
        descriptor,
        options,
        Encoding::Plain,
    )
}
