use std::sync::Arc;

use parquet2::{
    encoding::{hybrid_rle::HybridRleDecoder, Encoding},
    metadata::{ColumnChunkMetaData, ColumnDescriptor},
    page::DataPage,
    read::levels::get_bit_width,
    FallibleStreamingIterator,
};

use super::super::nested_utils::*;
use super::super::utils;
use super::basic::read_required;
use crate::{
    array::{Array, BooleanArray},
    bitmap::{utils::BitmapIter, MutableBitmap},
    datatypes::DataType,
    error::{ArrowError, Result},
};

fn read_values<D, G>(
    def_levels: D,
    max_def: u32,
    mut new_values: G,
    values: &mut MutableBitmap,
    validity: &mut MutableBitmap,
) where
    D: Iterator<Item = u32>,
    G: Iterator<Item = bool>,
{
    def_levels.for_each(|def| {
        if def == max_def {
            values.push(new_values.next().unwrap());
            validity.push(true);
        } else if def == max_def - 1 {
            values.push(false);
            validity.push(false);
        }
    });
}

#[allow(clippy::too_many_arguments)]
fn read(
    rep_levels: &[u8],
    def_levels: &[u8],
    values_buffer: &[u8],
    additional: usize,
    rep_level_encoding: (&Encoding, i16),
    def_level_encoding: (&Encoding, i16),
    is_nullable: bool,
    nested: &mut Vec<Box<dyn Nested>>,
    values: &mut MutableBitmap,
    validity: &mut MutableBitmap,
) {
    let max_rep_level = rep_level_encoding.1 as u32;
    let max_def_level = def_level_encoding.1 as u32;

    match (rep_level_encoding.0, def_level_encoding.0) {
        (Encoding::Rle, Encoding::Rle) => {
            let rep_levels =
                HybridRleDecoder::new(rep_levels, get_bit_width(rep_level_encoding.1), additional);
            if is_nullable {
                let def_levels = HybridRleDecoder::new(
                    def_levels,
                    get_bit_width(def_level_encoding.1),
                    additional,
                );
                let new_values = BitmapIter::new(values_buffer, 0, additional);
                read_values(def_levels, max_def_level, new_values, values, validity)
            } else {
                read_required(values_buffer, additional, values)
            }

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

fn extend_from_page(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    is_nullable: bool,
    nested: &mut Vec<Box<dyn Nested>>,
    values: &mut MutableBitmap,
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

pub fn iter_to_array<I, E>(
    mut iter: I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
) -> Result<Box<dyn Array>>
where
    ArrowError: From<E>,
    I: FallibleStreamingIterator<Item = DataPage, Error = E>,
{
    let capacity = metadata.num_values() as usize;
    let mut values = MutableBitmap::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);

    let (mut nested, is_nullable) = init_nested(metadata.descriptor().base_type(), capacity);

    while let Some(page) = iter.next()? {
        extend_from_page(
            page,
            metadata.descriptor(),
            is_nullable,
            &mut nested,
            &mut values,
            &mut validity,
        )?
    }

    let inner_data_type = match data_type {
        DataType::List(ref inner) => inner.data_type(),
        DataType::LargeList(ref inner) => inner.data_type(),
        _ => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Read nested datatype {:?}",
                data_type
            )))
        }
    };

    let values = Arc::new(BooleanArray::from_data(
        inner_data_type.clone(),
        values.into(),
        validity.into(),
    ));

    create_list(data_type, &mut nested, values)
}
