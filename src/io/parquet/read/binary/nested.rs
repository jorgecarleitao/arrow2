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
use super::basic::read_plain_required;
use crate::{
    array::{Array, BinaryArray, Offset, Utf8Array},
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
    datatypes::DataType,
    error::{ArrowError, Result},
};

fn read_values<'a, O, D, G>(
    def_levels: D,
    max_def: u32,
    mut new_values: G,
    offsets: &mut MutableBuffer<O>,
    values: &mut MutableBuffer<u8>,
    validity: &mut MutableBitmap,
) where
    O: Offset,
    D: Iterator<Item = u32>,
    G: Iterator<Item = &'a [u8]>,
{
    def_levels.for_each(|def| {
        if def == max_def {
            let v = new_values.next().unwrap();
            values.extend_from_slice(v);
            offsets.push(*offsets.last().unwrap() + O::from_usize(v.len()).unwrap());
            validity.push(true);
        } else if def == max_def - 1 {
            offsets.push(*offsets.last().unwrap());
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
    offsets: &mut MutableBuffer<O>,
    values: &mut MutableBuffer<u8>,
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
                let new_values = utils::BinaryIter::new(values_buffer);
                read_values(
                    def_levels,
                    max_def_level,
                    new_values,
                    offsets,
                    values,
                    validity,
                )
            } else {
                read_plain_required(values_buffer, additional, offsets, values)
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

fn extend_from_page<O: Offset>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    is_nullable: bool,
    nested: &mut Vec<Box<dyn Nested>>,
    offsets: &mut MutableBuffer<O>,
    values: &mut MutableBuffer<u8>,
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
                        offsets,
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
                    offsets,
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

pub fn iter_to_array<O, I, E>(
    mut iter: I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
) -> Result<Box<dyn Array>>
where
    O: Offset,
    ArrowError: From<E>,
    E: Clone,
    I: StreamingIterator<Item = std::result::Result<DataPage, E>>,
{
    let capacity = metadata.num_values() as usize;
    let mut values = MutableBuffer::<u8>::with_capacity(0);
    let mut offsets = MutableBuffer::<O>::with_capacity(1 + capacity);
    offsets.push(O::default());
    let mut validity = MutableBitmap::with_capacity(capacity);

    let (mut nested, is_nullable) = init_nested(metadata.descriptor().base_type(), capacity);

    while let Some(page) = iter.next() {
        extend_from_page(
            page.as_ref().map_err(|x| x.clone())?,
            metadata.descriptor(),
            is_nullable,
            &mut nested,
            &mut offsets,
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

    let values = match inner_data_type {
        DataType::LargeBinary | DataType::Binary => Arc::new(BinaryArray::from_data(
            offsets.into(),
            values.into(),
            validity.into(),
        )) as Arc<dyn Array>,
        DataType::LargeUtf8 | DataType::Utf8 => Arc::new(Utf8Array::from_data(
            offsets.into(),
            values.into(),
            validity.into(),
        )) as Arc<dyn Array>,
        _ => unreachable!(),
    };

    create_list(data_type, &mut nested, values)
}
