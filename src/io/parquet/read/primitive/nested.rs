use parquet2::{
    encoding::Encoding,
    read::{
        levels::{get_bit_width, split_buffer_v1, split_buffer_v2, RLEDecoder},
        Page, PageHeader,
    },
    types::NativeType,
};

use super::ColumnDescriptor;
use super::{super::utils, utils::ExactChunksIter, Nested};
use crate::{
    bitmap::MutableBitmap, buffer::MutableBuffer, error::Result,
    types::NativeType as ArrowNativeType,
};

fn compose_array<T, R, D, G, F, A>(
    mut rep_levels: R,
    mut def_levels: D,
    max_rep: u32,
    max_def: u32,
    mut new_values: G,
    op: F,
    nested: &mut Vec<Nested>,
    values: &mut MutableBuffer<A>,
    validity: &mut MutableBitmap,
) where
    T: NativeType,
    R: Iterator<Item = u32>,
    D: Iterator<Item = u32>,
    G: Iterator<Item = T>,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
    assert_eq!(max_rep, 1);
    assert_eq!(max_def, 3);
    let mut prev_rep = 0;
    let mut prev_def = 0;
    // [[0, 1], None, [2, None, 3], [4, 5, 6], [], [7, 8, 9], None, [10]]
    rep_levels.zip(def_levels).for_each(|(rep, def)| {
        match rep {
            // todo: generalize
            1 => {}
            0 => {
                if rep < prev_rep {
                    let level = (prev_rep - rep).saturating_sub(1) as usize;
                    nested[level].offsets.push(values.len() as i64);
                    nested[level].validity.push(true);
                }
            }
            _ => unreachable!(),
        }
        if def == max_def {
            values.push(op(new_values.next().unwrap()));
            validity.push(true);
        } else if def == max_def - 1 {
            values.push(A::default());
            validity.push(false);
        } else {
            // a null in the nested
            let level = (max_def - def).saturating_sub(3) as usize;
            let offsets = &mut nested[level].offsets;
            let last_offset = offsets[offsets.len() - 1];
            offsets.push(last_offset);
            nested[level].validity.push(def != 0);
        }
        prev_rep = rep;
        prev_def = def;
    });
    if prev_rep < max_rep {
        // todo: generalize
        nested[0].offsets.push(values.len() as i64);
        nested[0].validity.push(prev_def != 0);
    }
}

fn read<T, A, F>(
    rep_levels: &[u8],
    def_levels: &[u8],
    values_buffer: &[u8],
    additional: usize,
    rep_level_encoding: (&Encoding, i16),
    def_level_encoding: (&Encoding, i16),
    nested: &mut Vec<Nested>,
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

    match (
        (rep_level_encoding.0, max_rep_level == 0),
        (def_level_encoding.0, max_def_level == 0),
    ) {
        /*
        ((Encoding::Rle, true), (Encoding::Rle, true)) => compose_array(
            std::iter::repeat(0).take(additional),
            std::iter::repeat(0).take(additional),
            max_rep_level,
            max_def_level,
            new_values,
        ),
        ((Encoding::Rle, false), (Encoding::Rle, true)) => {
            let num_bits = get_bit_width(rep_level_encoding.1);
            let rep_levels = RLEDecoder::new(rep_levels, num_bits, additional as u32);
            compose_array(
                rep_levels,
                std::iter::repeat(0).take(additional),
                max_rep_level,
                max_def_level,
                new_values,
            )
        }
        ((Encoding::Rle, true), (Encoding::Rle, false)) => {
            let num_bits = get_bit_width(def_level_encoding.1);
            let def_levels = RLEDecoder::new(def_levels, num_bits, additional as u32);
            compose_array(
                std::iter::repeat(0).take(additional),
                def_levels,
                max_rep_level,
                max_def_level,
                values,
            )
        }
        */
        ((Encoding::Rle, false), (Encoding::Rle, false)) => {
            let rep_levels = RLEDecoder::new(
                rep_levels,
                get_bit_width(rep_level_encoding.1),
                additional as u32,
            );
            let def_levels = RLEDecoder::new(
                def_levels,
                get_bit_width(def_level_encoding.1),
                additional as u32,
            );
            compose_array(
                rep_levels,
                def_levels,
                max_rep_level,
                max_def_level,
                new_values,
                op,
                nested,
                values,
                validity,
            )
        }
        _ => todo!(),
    }
}

pub fn extend_from_page<T, A, F>(
    page: &Page,
    descriptor: &ColumnDescriptor,
    nested: &mut Vec<Nested>,
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

    let is_optional = descriptor.max_def_level() == 1;
    match page.header() {
        PageHeader::V1(header) => {
            assert_eq!(header.definition_level_encoding, Encoding::Rle);
            assert_eq!(header.repetition_level_encoding, Encoding::Rle);

            match (&page.encoding(), page.dictionary_page(), is_optional) {
                (Encoding::Plain, None, true) => {
                    let (rep_levels, def_levels, values_buffer) =
                        split_buffer_v1(page.buffer(), false, is_optional);
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
                        nested,
                        values,
                        validity,
                        op,
                    )
                }
                (Encoding::Plain, None, false) => {
                    todo!()
                }
                _ => {
                    return Err(utils::not_implemented(
                        &page.encoding(),
                        is_optional,
                        page.dictionary_page().is_some(),
                        "V1",
                        "primitive",
                    ))
                }
            }
        }
        PageHeader::V2(header) => match (&page.encoding(), page.dictionary_page()) {
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
                    nested,
                    values,
                    validity,
                    op,
                )
            }
            _ => {
                return Err(utils::not_implemented(
                    &page.encoding(),
                    is_optional,
                    page.dictionary_page().is_some(),
                    "V2",
                    "primitive",
                ))
            }
        },
    };
    Ok(())
}
