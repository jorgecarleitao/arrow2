use parquet2::schema::Encoding;
use parquet2::{metadata::ColumnDescriptor, page::CompressedDataPage, write::WriteOptions};

use super::super::{levels, utils};
use super::basic::{build_statistics, encode_plain};
use crate::{
    array::{Array, Offset, Utf8Array},
    error::Result,
    io::parquet::read::is_type_nullable,
};

pub fn array_to_page<O, OO>(
    array: &Utf8Array<O>,
    options: WriteOptions,
    descriptor: ColumnDescriptor,
    nested: levels::NestedInfo<OO>,
) -> Result<CompressedDataPage>
where
    OO: Offset,
    O: Offset,
{
    let is_optional = is_type_nullable(descriptor.type_());

    let validity = array.validity();

    let mut buffer = vec![];
    levels::write_rep_levels(&mut buffer, &nested, options.version)?;
    let repetition_levels_byte_length = buffer.len();

    levels::write_def_levels(&mut buffer, &nested, validity, options.version)?;
    let definition_levels_byte_length = buffer.len() - repetition_levels_byte_length;

    encode_plain(array, is_optional, &mut buffer);

    let uncompressed_page_size = buffer.len();

    let buffer = utils::compress(
        buffer,
        options,
        definition_levels_byte_length + repetition_levels_byte_length,
    )?;

    let statistics = if options.write_statistics {
        Some(build_statistics(array, descriptor.clone()))
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
        Encoding::Plain,
    )
}
