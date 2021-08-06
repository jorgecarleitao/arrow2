use std::sync::Arc;

use parquet2::{
    encoding::Encoding,
    metadata::{ColumnChunkMetaData, ColumnDescriptor},
    page::{DataPage, DataPageHeader},
    read::{
        levels::{get_bit_width, split_buffer_v1, split_buffer_v2, RLEDecoder},
        StreamingIterator,
    },
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
            let rep_levels = RLEDecoder::new(
                rep_levels,
                get_bit_width(rep_level_encoding.1),
                additional as u32,
            );
            if is_nullable {
                let def_levels = RLEDecoder::new(
                    def_levels,
                    get_bit_width(def_level_encoding.1),
                    additional as u32,
                );
                let new_values = BitmapIter::new(values_buffer, 0, additional);
                read_values(def_levels, max_def_level, new_values, values, validity)
            } else {
                read_required(values_buffer, additional, values)
            }

            let def_levels = RLEDecoder::new(
                def_levels,
                get_bit_width(def_level_encoding.1),
                additional as u32,
            );

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

    match page.header() {
        DataPageHeader::V1(header) => {
            assert_eq!(header.definition_level_encoding, Encoding::Rle);
            assert_eq!(header.repetition_level_encoding, Encoding::Rle);

            match (&page.encoding(), page.dictionary_page()) {
                (Encoding::Plain, None) => {
                    let (rep_levels, def_levels, values_buffer) = split_buffer_v1(
                        page.buffer(),
                        descriptor.max_rep_level() > 0,
                        descriptor.max_def_level() > 0,
                    );
                    read(
                        rep_levels,
                        def_levels,
                        values_buffer,
                        additional,
                        (
                            &header.repetition_level_encoding,
                            descriptor.max_rep_level(),
                        ),
                        (
                            &header.definition_level_encoding,
                            descriptor.max_def_level(),
                        ),
                        is_nullable,
                        nested,
                        values,
                        validity,
                    )
                }
                _ => {
                    return Err(utils::not_implemented(
                        &page.encoding(),
                        is_nullable,
                        page.dictionary_page().is_some(),
                        "V1",
                        "primitive",
                    ))
                }
            }
        }
        DataPageHeader::V2(header) => match (&page.encoding(), page.dictionary_page()) {
            (Encoding::Plain, None) => {
                let def_level_buffer_length = header.definition_levels_byte_length as usize;
                let rep_level_buffer_length = header.repetition_levels_byte_length as usize;
                let (rep_levels, def_levels, values_buffer) = split_buffer_v2(
                    page.buffer(),
                    rep_level_buffer_length,
                    def_level_buffer_length,
                );
                read(
                    rep_levels,
                    def_levels,
                    values_buffer,
                    additional,
                    (&Encoding::Rle, descriptor.max_rep_level()),
                    (&Encoding::Rle, descriptor.max_def_level()),
                    is_nullable,
                    nested,
                    values,
                    validity,
                )
            }
            _ => {
                return Err(utils::not_implemented(
                    &page.encoding(),
                    is_nullable,
                    page.dictionary_page().is_some(),
                    "V2",
                    "primitive",
                ))
            }
        },
    };
    Ok(())
}

pub fn iter_to_array<I, E>(
    mut iter: I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
) -> Result<Box<dyn Array>>
where
    ArrowError: From<E>,
    E: Clone,
    I: StreamingIterator<Item = std::result::Result<DataPage, E>>,
{
    let capacity = metadata.num_values() as usize;
    let mut values = MutableBitmap::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);

    let (mut nested, is_nullable) = init_nested(metadata.descriptor().base_type(), capacity);

    while let Some(page) = iter.next() {
        extend_from_page(
            page.as_ref().map_err(|x| x.clone())?,
            metadata.descriptor(),
            is_nullable,
            &mut nested,
            &mut values,
            &mut validity,
        )?
    }

    let values = Arc::new(BooleanArray::from_data(values.into(), validity.into()));

    create_list(data_type, &mut nested, values)
}
