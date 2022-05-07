use parquet2::encoding::hybrid_rle::encode_u32;
use parquet2::write::Version;

use crate::{array::Offset, bitmap::Bitmap, error::Result};

pub fn num_values<O: Offset>(offsets: &[O]) -> usize {
    offsets
        .windows(2)
        .map(|w| {
            let length = w[1].to_usize() - w[0].to_usize();
            if length == 0 {
                1
            } else {
                length
            }
        })
        .sum()
}

/// Iterator adapter of parquet / dremel repetition levels
#[derive(Debug)]
pub struct RepLevelsIter<'a, O: Offset> {
    iter: std::slice::Windows<'a, O>,
    remaining: usize,
    length: usize,
    total_size: usize,
}

impl<'a, O: Offset> RepLevelsIter<'a, O> {
    pub fn new(offsets: &'a [O]) -> Self {
        let total_size = num_values(offsets);

        Self {
            iter: offsets.windows(2),
            remaining: 0,
            length: 0,
            total_size,
        }
    }
}

impl<O: Offset> Iterator for RepLevelsIter<'_, O> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == self.length {
            if let Some(w) = self.iter.next() {
                let start = w[0].to_usize();
                let end = w[1].to_usize();
                self.length = end - start;
                self.remaining = 0;
                if self.length == 0 {
                    self.total_size -= 1;
                    return Some(0);
                }
            } else {
                return None;
            }
        }
        let old = self.remaining;
        self.remaining += 1;
        self.total_size -= 1;
        Some((old >= 1) as u32)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.total_size, Some(self.total_size))
    }
}

/// Iterator adapter of parquet / dremel definition levels
pub struct DefLevelsIter<'a, O: Offset, II: Iterator<Item = u32>, I: Iterator<Item = u32>> {
    iter: std::iter::Zip<std::slice::Windows<'a, O>, II>,
    primitive_validity: I,
    remaining: usize,
    is_valid: u32,
    total_size: usize,
}

impl<'a, O: Offset, II: Iterator<Item = u32>, I: Iterator<Item = u32>> DefLevelsIter<'a, O, II, I> {
    pub fn new(offsets: &'a [O], validity: II, primitive_validity: I) -> Self {
        let total_size = num_values(offsets);

        let iter = offsets.windows(2).zip(validity);

        Self {
            iter,
            primitive_validity,
            remaining: 0,
            is_valid: 0,
            total_size,
        }
    }
}

impl<O: Offset, II: Iterator<Item = u32>, I: Iterator<Item = u32>> Iterator
    for DefLevelsIter<'_, O, II, I>
{
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            let (w, is_valid) = self.iter.next()?;
            let start = w[0].to_usize();
            let end = w[1].to_usize();
            self.remaining = end - start;
            self.is_valid = is_valid + 1;
            if self.remaining == 0 {
                self.total_size -= 1;
                return Some(self.is_valid - 1);
            }
        }
        self.remaining -= 1;
        self.total_size -= 1;

        let p_is_valid = self.primitive_validity.next().unwrap_or_default();
        Some(self.is_valid + p_is_valid)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.total_size, Some(self.total_size))
    }
}

#[derive(Debug)]
pub struct NestedInfo<'a, O: Offset> {
    is_optional: bool,
    offsets: &'a [O],
    validity: Option<&'a Bitmap>,
}

impl<'a, O: Offset> NestedInfo<'a, O> {
    pub fn new(offsets: &'a [O], validity: Option<&'a Bitmap>, is_optional: bool) -> Self {
        Self {
            is_optional,
            offsets,
            validity,
        }
    }

    pub fn offsets(&self) -> &'a [O] {
        self.offsets
    }
}

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
pub fn write_rep_levels<O: Offset>(
    buffer: &mut Vec<u8>,
    nested: &NestedInfo<O>,
    version: Version,
) -> Result<()> {
    let num_bits = 1; // todo: compute this

    match version {
        Version::V1 => {
            write_levels_v1(buffer, |buffer: &mut Vec<u8>| {
                let levels = RepLevelsIter::new(nested.offsets);
                encode_u32(buffer, levels, num_bits)?;
                Ok(())
            })?;
        }
        Version::V2 => {
            let levels = RepLevelsIter::new(nested.offsets);

            encode_u32(buffer, levels, num_bits)?;
        }
    }

    Ok(())
}

fn write_def_levels1<I: Iterator<Item = u32>>(
    buffer: &mut Vec<u8>,
    levels: I,
    num_bits: u8,
    version: Version,
) -> Result<()> {
    match version {
        Version::V1 => write_levels_v1(buffer, move |buffer: &mut Vec<u8>| {
            encode_u32(buffer, levels, num_bits)?;
            Ok(())
        }),
        Version::V2 => Ok(encode_u32(buffer, levels, num_bits)?),
    }
}

/// writes the rep levels to a `Vec<u8>`.
pub fn write_def_levels<O: Offset>(
    buffer: &mut Vec<u8>,
    nested: &NestedInfo<O>,
    validity: Option<&Bitmap>,
    primitive_is_optional: bool,
    version: Version,
) -> Result<()> {
    let mut num_bits = 1 + nested.is_optional as u8 + primitive_is_optional as u8;
    if num_bits == 3 {
        // brute-force log2 - this needs to be generalized for e.g. list of list
        num_bits = 2
    };

    // this match ensures that irrespectively of the arrays' validities, we write def levels
    // that are consistent with the declared parquet schema.
    // see comments on some of the variants
    match (
        nested.is_optional,
        nested.validity.as_ref(),
        primitive_is_optional,
        validity.as_ref(),
    ) {
        // if the validity is optional and there is no validity in the array, we
        // need to write 1 to mark the fields as valid
        (true, None, true, None) => {
            let nested_validity = std::iter::repeat(1);
            let validity = std::iter::repeat(1);
            let levels = DefLevelsIter::new(nested.offsets, nested_validity, validity);
            write_def_levels1(buffer, levels, num_bits, version)
        }
        (true, Some(nested_validity), true, None) => {
            let levels = DefLevelsIter::new(
                nested.offsets,
                nested_validity.iter().map(|x| x as u32),
                std::iter::repeat(1),
            );
            write_def_levels1(buffer, levels, num_bits, version)
        }
        (true, None, true, Some(validity)) => {
            let levels = DefLevelsIter::new(
                nested.offsets,
                std::iter::repeat(1),
                validity.iter().map(|x| x as u32),
            );
            write_def_levels1(buffer, levels, num_bits, version)
        }
        (true, Some(nested_validity), true, Some(validity)) => {
            let levels = DefLevelsIter::new(
                nested.offsets,
                nested_validity.iter().map(|x| x as u32),
                validity.iter().map(|x| x as u32),
            );
            write_def_levels1(buffer, levels, num_bits, version)
        }
        (true, None, false, _) => {
            let nested_validity = std::iter::repeat(1);
            let validity = std::iter::repeat(0);
            let levels = DefLevelsIter::new(nested.offsets, nested_validity, validity);
            write_def_levels1(buffer, levels, num_bits, version)
        }
        (true, Some(nested_validity), false, _) => {
            let nested_validity = nested_validity.iter().map(|x| x as u32);
            let validity = std::iter::repeat(0);
            let levels = DefLevelsIter::new(nested.offsets, nested_validity, validity);
            write_def_levels1(buffer, levels, num_bits, version)
        }
        (false, _, true, None) => {
            let nested_validity = std::iter::repeat(0);
            let validity = std::iter::repeat(1);
            let levels = DefLevelsIter::new(nested.offsets, nested_validity, validity);
            write_def_levels1(buffer, levels, num_bits, version)
        }
        (false, _, true, Some(validity)) => {
            let nested_validity = std::iter::repeat(0);
            let validity = validity.iter().map(|x| x as u32);
            let levels = DefLevelsIter::new(nested.offsets, nested_validity, validity);
            write_def_levels1(buffer, levels, num_bits, version)
        }
        (false, _, false, _) => {
            let nested_validity = std::iter::repeat(0);
            let validity = std::iter::repeat(0);
            let levels = DefLevelsIter::new(nested.offsets, nested_validity, validity);
            write_def_levels1(buffer, levels, num_bits, version)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rep_levels() {
        let offsets = [0, 2, 2, 5, 8, 8, 11, 11, 12].as_ref();
        let expected = vec![0u32, 1, 0, 0, 1, 1, 0, 1, 1, 0, 0, 1, 1, 0, 0];

        let result = RepLevelsIter::new(offsets).collect::<Vec<_>>();
        assert_eq!(result, expected)
    }

    #[test]
    fn test_def_levels() {
        // [[0, 1], None, [2, None, 3], [4, 5, 6], [], [7, 8, 9], None, [10]]
        let offsets = [0, 2, 2, 5, 8, 8, 11, 11, 12].as_ref();
        let validity = [true, false, true, true, true, true, false, true]
            .into_iter()
            .map(|x| x as u32);
        let primitive_validity = [
            true, true, //[0, 1]
            true, false, true, //[2, None, 3]
            true, true, true, //[4, 5, 6]
            true, true, true, //[7, 8, 9]
            true, //[10]
        ]
        .into_iter()
        .map(|x| x as u32);
        let expected = vec![3u32, 3, 0, 3, 2, 3, 3, 3, 3, 1, 3, 3, 3, 0, 3];

        let result = DefLevelsIter::new(offsets, validity, primitive_validity).collect::<Vec<_>>();
        assert_eq!(result, expected)
    }

    #[test]
    fn test_def_levels1() {
        // [[0, 1], [], [2, 0, 3], [4, 5, 6], [], [7, 8, 9], [], [10]]
        let offsets = [0, 2, 2, 5, 8, 8, 11, 11, 12].as_ref();
        let validity = std::iter::repeat(0);
        let primitive_validity = std::iter::repeat(0);
        let expected = vec![1u32, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0, 1];

        let result = DefLevelsIter::new(offsets, validity, primitive_validity).collect::<Vec<_>>();
        assert_eq!(result, expected)
    }
}
