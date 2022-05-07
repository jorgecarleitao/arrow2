use parquet2::schema::types::PrimitiveType;
use parquet2::{encoding::Encoding, page::DataPage};

use super::super::{levels, utils, WriteOptions};
use super::basic::{build_statistics, encode_plain};
use crate::io::parquet::read::schema::is_nullable;
use crate::{
    array::{Array, BinaryArray, Offset},
    error::Result,
};

pub fn array_to_page<O, OO>(
    array: &BinaryArray<O>,
    options: WriteOptions,
    type_: PrimitiveType,
    nested: levels::NestedInfo<OO>,
) -> Result<DataPage>
where
    OO: Offset,
    O: Offset,
{
    let is_optional = is_nullable(&type_.field_info);

    let validity = array.validity();

    let mut buffer = vec![];
    levels::write_rep_levels(&mut buffer, &nested, options.version)?;
    let repetition_levels_byte_length = buffer.len();

    levels::write_def_levels(&mut buffer, &nested, validity, is_optional, options.version)?;
    let definition_levels_byte_length = buffer.len() - repetition_levels_byte_length;

    encode_plain(array, is_optional, &mut buffer);

    let statistics = if options.write_statistics {
        Some(build_statistics(array, type_.clone()))
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
        type_,
        options,
        Encoding::Plain,
    )
}
