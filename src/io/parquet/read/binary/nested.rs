use parquet2::{
    encoding::{hybrid_rle::HybridRleDecoder, Encoding},
    metadata::ColumnDescriptor,
    page::DataPage,
    read::levels::get_bit_width,
};

use super::super::utils;
use super::super::utils::Pushable;
use super::{super::nested_utils::*, utils::Binary};

use crate::{array::Offset, bitmap::MutableBitmap, error::Result};

fn read_plain_required<O: Offset>(buffer: &[u8], additional: usize, values: &mut Binary<O>) {
    let values_iterator = utils::BinaryIter::new(buffer);

    // each value occupies 4 bytes + len declared in 4 bytes => reserve accordingly.
    values.offsets.reserve(additional);
    values.values.reserve(buffer.len() - 4 * additional);
    let a = values.values.capacity();
    for value in values_iterator {
        values.push(value);
    }
    debug_assert_eq!(a, values.values.capacity());
}

fn read_values<'a, O, D, G>(
    def_levels: D,
    max_def: u32,
    mut new_values: G,
    values: &mut Binary<O>,
    validity: &mut MutableBitmap,
) where
    O: Offset,
    D: Iterator<Item = u32>,
    G: Iterator<Item = &'a [u8]>,
{
    def_levels.for_each(|def| {
        if def == max_def {
            let v = new_values.next().unwrap();
            values.push(v);
            validity.push(true);
        } else if def == max_def - 1 {
            values.push(&[]);
            validity.push(false);
        }
    });
}

#[allow(clippy::too_many_arguments)]
fn read<O: Offset>(
    rep_levels: &[u8],
    def_levels: &[u8],
    values_buffer: &[u8],
    additional: usize,
    rep_level_encoding: (&Encoding, i16),
    def_level_encoding: (&Encoding, i16),
    is_nullable: bool,
    nested: &mut Vec<Box<dyn Nested>>,
    values: &mut Binary<O>,
    validity: &mut MutableBitmap,
) {
    let max_rep_level = rep_level_encoding.1 as u32;
    let max_def_level = def_level_encoding.1 as u32;

    match (rep_level_encoding.0, def_level_encoding.0) {
        (Encoding::Rle, Encoding::Rle) => {
            if is_nullable {
                let def_levels = HybridRleDecoder::new(
                    def_levels,
                    get_bit_width(def_level_encoding.1),
                    additional,
                );
                let new_values = utils::BinaryIter::new(values_buffer);
                read_values(def_levels, max_def_level, new_values, values, validity)
            } else {
                read_plain_required(values_buffer, additional, values)
            }

            let rep_levels =
                HybridRleDecoder::new(rep_levels, get_bit_width(rep_level_encoding.1), additional);
            let def_levels =
                HybridRleDecoder::new(def_levels, get_bit_width(def_level_encoding.1), additional);

            extend_offsets(
                rep_levels,
                def_levels,
                is_nullable,
                max_rep_level,
                max_def_level,
                nested,
            )
        }
        _ => todo!(),
    }
}

pub(super) fn extend_from_page<O: Offset>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    is_nullable: bool,
    nested: &mut Vec<Box<dyn Nested>>,
    values: &mut Binary<O>,
    validity: &mut MutableBitmap,
) -> Result<()> {
    let additional = page.num_values();

    let (rep_levels, def_levels, values_buffer, version) = utils::split_buffer(page, descriptor);

    match (&page.encoding(), page.dictionary_page()) {
        (Encoding::Plain, None) => read(
            rep_levels,
            def_levels,
            values_buffer,
            additional,
            (
                &page.repetition_level_encoding(),
                descriptor.max_rep_level(),
            ),
            (
                &page.definition_level_encoding(),
                descriptor.max_def_level(),
            ),
            is_nullable,
            nested,
            values,
            validity,
        ),
        _ => {
            return Err(utils::not_implemented(
                &page.encoding(),
                is_nullable,
                page.dictionary_page().is_some(),
                version,
                "primitive",
            ))
        }
    }
    Ok(())
}
