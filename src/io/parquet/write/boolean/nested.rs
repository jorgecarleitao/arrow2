use parquet2::{metadata::ColumnDescriptor, read::CompressedPage, write::WriteOptions};

use super::super::{levels, utils};
use super::basic::{build_statistics, encode};
use crate::{
    array::{Array, BooleanArray, Offset},
    error::Result,
    io::parquet::read::is_type_nullable,
};

pub fn array_to_page<O>(
    array: &BooleanArray,
    options: WriteOptions,
    descriptor: ColumnDescriptor,
    nested: levels::NestedInfo<O>,
) -> Result<CompressedPage>
where
    O: Offset,
{
    let is_optional = is_type_nullable(descriptor.type_());

    let validity = array.validity();

    let mut buffer = vec![];
    levels::write_rep_levels(&mut buffer, &nested, options.version)?;
    let repetition_levels_byte_length = buffer.len();

    levels::write_def_levels(&mut buffer, &nested, validity, options.version)?;
    let definition_levels_byte_length = buffer.len() - repetition_levels_byte_length;

    if is_optional {
        let iter = array.iter().flatten().take(
            validity
                .as_ref()
                .map(|x| x.len() - x.null_count())
                .unwrap_or_else(|| array.len()),
        );
        encode(iter, &mut buffer)
    } else {
        let iter = array.values().iter();
        encode(iter, &mut buffer)
    }?;

    let uncompressed_page_size = buffer.len();

    let buffer = utils::compress(
        buffer,
        options,
        definition_levels_byte_length + repetition_levels_byte_length,
    )?;

    let statistics = if options.write_statistics {
        Some(build_statistics(array))
    } else {
        None
    };

    utils::build_plain_page(
        buffer,
        levels::num_values(nested.offsets()),
        array.null_count(),
        uncompressed_page_size,
        repetition_levels_byte_length,
        definition_levels_byte_length,
        statistics,
        descriptor,
        options,
    )
}
