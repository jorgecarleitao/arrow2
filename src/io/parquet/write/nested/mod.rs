mod def;
mod rep;

use parquet2::{encoding::hybrid_rle::encode_u32, read::levels::get_bit_width, write::Version};

use crate::{array::Offset, error::Result};

use super::Nested;

pub use rep::num_values;

fn write_levels_v1<F: FnOnce(&mut Vec<u8>) -> Result<()>>(
    buffer: &mut Vec<u8>,
    encode: F,
) -> Result<()> {
    buffer.extend_from_slice(&[0; 4]);
    let start = buffer.len();

    encode(buffer)?;

    let end = buffer.len();
    let length = end - start;

    // write the first 4 bytes as length
    let length = (length as i32).to_le_bytes();
    (0..4).for_each(|i| buffer[start - 4 + i] = length[i]);
    Ok(())
}

/// writes the rep levels to a `Vec<u8>`.
pub fn write_rep_levels(buffer: &mut Vec<u8>, nested: &[Nested], version: Version) -> Result<()> {
    let num_bits = get_bit_width(max_rep_level(nested) as i16) as u8;

    match version {
        Version::V1 => {
            write_levels_v1(buffer, |buffer: &mut Vec<u8>| {
                let levels = rep::RepLevelsIter::new(nested);
                encode_u32(buffer, levels, num_bits)?;
                Ok(())
            })?;
        }
        Version::V2 => {
            let levels = rep::RepLevelsIter::new(nested);
            encode_u32(buffer, levels, num_bits)?;
        }
    }

    Ok(())
}

/// writes the rep levels to a `Vec<u8>`.
pub fn write_def_levels(buffer: &mut Vec<u8>, nested: &[Nested], version: Version) -> Result<()> {
    let num_bits = get_bit_width(max_def_level(nested) as i16) as u8;

    let levels = def::DefLevelsIter::new(nested);

    match version {
        Version::V1 => write_levels_v1(buffer, move |buffer: &mut Vec<u8>| {
            encode_u32(buffer, levels, num_bits)?;
            Ok(())
        }),
        Version::V2 => Ok(encode_u32(buffer, levels, num_bits)?),
    }
}

fn max_def_level(nested: &[Nested]) -> usize {
    nested
        .iter()
        .map(|nested| match nested {
            Nested::Primitive(_, is_optional, _) => *is_optional as usize,
            Nested::List(nested) => 1 + (nested.is_optional as usize),
            Nested::LargeList(nested) => 1 + (nested.is_optional as usize),
            Nested::Struct(_, is_optional, _) => *is_optional as usize,
        })
        .sum()
}

fn max_rep_level(nested: &[Nested]) -> usize {
    nested
        .iter()
        .map(|nested| match nested {
            Nested::LargeList(_) | Nested::List(_) => 1,
            Nested::Primitive(_, _, _) | Nested::Struct(_, _, _) => 0,
        })
        .sum()
}

fn to_length<O: Offset>(
    offsets: &[O],
) -> impl Iterator<Item = usize> + std::fmt::Debug + Clone + '_ {
    offsets
        .windows(2)
        .map(|w| w[1].to_usize() - w[0].to_usize())
}
