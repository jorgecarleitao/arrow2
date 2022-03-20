use parquet2::metadata::Descriptor;
use parquet2::{encoding::Encoding, page::DataPage, write::WriteOptions};

use super::super::{levels, utils};
use super::basic::{build_statistics, encode_plain};
use crate::io::parquet::read::schema::is_nullable;
use crate::{
    array::{Array, BinaryArray, Offset},
    error::Result,
};

pub fn array_to_page<O, OO>(
    array: &BinaryArray<O>,
    options: WriteOptions,
    descriptor: Descriptor,
    nested: levels::NestedInfo<OO>,
) -> Result<DataPage>
where
    OO: Offset,
    O: Offset,
{
    let is_optional = is_nullable(&descriptor.primitive_type.field_info);

    let validity = array.validity();

    let mut buffer = vec![];
    levels::write_rep_levels(&mut buffer, &nested, options.version)?;
    let repetition_levels_byte_length = buffer.len();

    levels::write_def_levels(&mut buffer, &nested, validity, options.version)?;
    let definition_levels_byte_length = buffer.len() - repetition_levels_byte_length;

    encode_plain(array, is_optional, &mut buffer);

    let statistics = if options.write_statistics {
        Some(build_statistics(array, descriptor.primitive_type.clone()))
    } else {
        None
    };

    utils::build_plain_page(
        buffer,
        levels::num_values(nested.offsets()),
        nested.offsets().len().saturating_sub(1),
        array.null_count(),
        repetition_levels_byte_length,
        definition_levels_byte_length,
        statistics,
        descriptor,
        options,
        Encoding::Plain,
    )
}
