use parquet2::{
    encoding::Encoding,
    page::{DataPage, DataPageHeader},
    read::levels::{get_bit_width, split_buffer_v1, split_buffer_v2, RLEDecoder},
    types::NativeType,
};

use super::super::nested_utils::extend_offsets;
use super::ColumnDescriptor;
use super::{super::utils, utils::ExactChunksIter, Nested};
use crate::{
    bitmap::MutableBitmap, buffer::MutableBuffer, error::Result, trusted_len::TrustedLen,
    types::NativeType as ArrowNativeType,
};

fn read_values<T, D, G, F, A>(
    def_levels: D,
    max_def: u32,
    mut new_values: G,
    op: F,
    values: &mut MutableBuffer<A>,
    validity: &mut MutableBitmap,
) where
    T: NativeType,
    D: Iterator<Item = u32>,
    G: Iterator<Item = T>,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
    def_levels.for_each(|def| {
        if def == max_def {
            values.push(op(new_values.next().unwrap()));
            validity.push(true);
        } else if def == max_def - 1 {
            values.push(A::default());
            validity.push(false);
        }
    });
}

fn read_values_required<T, G, F, A>(new_values: G, op: F, values: &mut MutableBuffer<A>)
where
    T: NativeType,
    G: TrustedLen<Item = T>,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
    let iterator = new_values.map(|v| op(v));
    values.extend_from_trusted_len_iter(iterator);
}

#[allow(clippy::too_many_arguments)]
fn read<T, A, F>(
    rep_levels: &[u8],
    def_levels: &[u8],
    values_buffer: &[u8],
    additional: usize,
    rep_level_encoding: (&Encoding, i16),
    def_level_encoding: (&Encoding, i16),
    is_nullable: bool,
    nested: &mut Vec<Box<dyn Nested>>,
    values: &mut MutableBuffer<A>,
    validity: &mut MutableBitmap,
    op: F,
) where
    T: NativeType,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
    let new_values = ExactChunksIter::<T>::new(values_buffer);

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
                read_values(def_levels, max_def_level, new_values, op, values, validity)
            } else {
                read_values_required(new_values, op, values)
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

pub fn extend_from_page<T, A, F>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    is_nullable: bool,
    nested: &mut Vec<Box<dyn Nested>>,
    values: &mut MutableBuffer<A>,
    validity: &mut MutableBitmap,
    op: F,
) -> Result<()>
where
    T: NativeType,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
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
                        op,
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
                    op,
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
