use parquet2::encoding::hybrid_rle::encode_u32;
use parquet2::write::Version;

use crate::{array::Offset, bitmap::Bitmap, error::Result};

pub fn num_values_iter<I: Iterator<Item = usize>>(lengths: I) -> usize {
    lengths
        .map(|length| if length == 0 { 1 } else { length })
        .sum()
}

pub fn to_length<O: Offset>(
    offsets: &[O],
) -> impl Iterator<Item = usize> + std::fmt::Debug + Clone + '_ {
    offsets
        .windows(2)
        .map(|w| w[1].to_usize() - w[0].to_usize())
}

pub fn num_values<O: Offset>(offsets: &[O]) -> usize {
    num_values_iter(to_length(offsets))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepetitionRecord {
    Empty,
    FirstItem(usize),
    Item,
}

impl From<RepetitionRecord> for u32 {
    #[inline]
    fn from(other: RepetitionRecord) -> Self {
        match other {
            RepetitionRecord::Empty => 0,
            RepetitionRecord::FirstItem(_) => 0,
            RepetitionRecord::Item => 1,
        }
    }
}

/// Iterator adapter of dremel repetition levels. The adapted iterator is assumed to be a sequence of lengths.
///
/// This iterator returns 0 or 1 depending on whether it is the start of the record.
/// For example, the lengths [0, 1, 2, 0] yield [0, 0, 0, 1, 0]:
/// * 0 -> Empty
/// * 1 -> 0
/// * 2 -> 0, 1
/// * 0 -> 0
#[derive(Debug)]
pub struct RepRecordIter<I: Iterator<Item = usize> + std::fmt::Debug + Clone> {
    iter: I,
    remaining: usize,
    length: usize,
    total_size: usize,
}

impl<I: Iterator<Item = usize> + std::fmt::Debug + Clone> RepRecordIter<I> {
    /// `iter` is expected to be a list of lengths.
    pub fn new(iter: I) -> Self {
        let total_size = num_values_iter(iter.clone());

        Self {
            iter,
            remaining: 0,
            length: 0,
            total_size,
        }
    }
}

impl<I: Iterator<Item = usize> + std::fmt::Debug + Clone> Iterator for RepRecordIter<I> {
    type Item = RepetitionRecord;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == self.length {
            self.length = self.iter.next()?;
            self.remaining = 0;
            if self.length == 0 {
                self.total_size -= 1;
                return Some(RepetitionRecord::Empty);
            }
        }
        let old = self.remaining;
        self.remaining += 1;
        self.total_size -= 1;
        Some(if old >= 1 {
            RepetitionRecord::Item
        } else {
            RepetitionRecord::FirstItem(self.length)
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.total_size, Some(self.total_size))
    }
}

/// Iterator adapter of dremel repetition levels. The adapted iterator is assumed to be a sequence of lengths.
///
/// This iterator returns 0 or 1 depending on whether it is the start of the record.
/// For example, the lengths [0, 1, 2, 0] yield [0, 0, 0, 1, 0]:
/// * 0 -> 0
/// * 1 -> 0
/// * 2 -> 0, 1
/// * 0 -> 0
#[derive(Debug)]
pub struct RepLevelsIter<I: Iterator<Item = usize> + std::fmt::Debug + Clone> {
    iter: RepRecordIter<I>,
}

impl<I: Iterator<Item = usize> + std::fmt::Debug + Clone> RepLevelsIter<I> {
    /// `iter` is expected to be a list of lengths.
    pub fn new(iter: I) -> Self {
        Self {
            iter: RepRecordIter::new(iter),
        }
    }
}

impl<I: Iterator<Item = usize> + std::fmt::Debug + Clone> Iterator for RepLevelsIter<I> {
    type Item = u32;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|x| x.into())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

/// Iterator adapter of parquet / dremel definition levels
pub struct DefLevelsIter<L, II, I>
where
    L: Iterator<Item = usize> + std::fmt::Debug + Clone,
    II: Iterator<Item = u32>,
    I: Iterator<Item = u32>,
{
    iter: std::iter::Zip<L, II>,
    primitive_validity: I,
    remaining: usize,
    is_valid: u32,
    total_size: usize,
}

impl<L, II, I> DefLevelsIter<L, II, I>
where
    L: Iterator<Item = usize> + std::fmt::Debug + Clone,
    II: Iterator<Item = u32>,
    I: Iterator<Item = u32>,
{
    pub fn new(lenghts: L, validity: II, primitive_validity: I) -> Self {
        let total_size = num_values_iter(lenghts.clone());

        let iter = lenghts.zip(validity);

        Self {
            iter,
            primitive_validity,
            remaining: 0,
            is_valid: 0,
            total_size,
        }
    }
}

impl<L, II, I> Iterator for DefLevelsIter<L, II, I>
where
    L: Iterator<Item = usize> + std::fmt::Debug + Clone,
    II: Iterator<Item = u32>,
    I: Iterator<Item = u32>,
{
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            let (length, is_valid) = self.iter.next()?;
            self.remaining = length;
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

#[derive(Debug, Clone, PartialEq)]
pub struct NestedInfo<'a, O: Offset> {
    pub is_optional: bool,
    pub offsets: &'a [O],
    pub validity: Option<&'a Bitmap>,
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

    let lengths = nested
        .offsets
        .windows(2)
        .map(|w| w[1].to_usize() - w[0].to_usize());

    match version {
        Version::V1 => {
            write_levels_v1(buffer, |buffer: &mut Vec<u8>| {
                let levels = RepLevelsIter::new(lengths);
                encode_u32(buffer, levels, num_bits)?;
                Ok(())
            })?;
        }
        Version::V2 => {
            let levels = RepLevelsIter::new(lengths);

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
            let levels = DefLevelsIter::new(to_length(nested.offsets), nested_validity, validity);
            write_def_levels1(buffer, levels, num_bits, version)
        }
        (true, Some(nested_validity), true, None) => {
            let levels = DefLevelsIter::new(
                to_length(nested.offsets),
                nested_validity.iter().map(|x| x as u32),
                std::iter::repeat(1),
            );
            write_def_levels1(buffer, levels, num_bits, version)
        }
        (true, None, true, Some(validity)) => {
            let levels = DefLevelsIter::new(
                to_length(nested.offsets),
                std::iter::repeat(1),
                validity.iter().map(|x| x as u32),
            );
            write_def_levels1(buffer, levels, num_bits, version)
        }
        (true, Some(nested_validity), true, Some(validity)) => {
            let levels = DefLevelsIter::new(
                to_length(nested.offsets),
                nested_validity.iter().map(|x| x as u32),
                validity.iter().map(|x| x as u32),
            );
            write_def_levels1(buffer, levels, num_bits, version)
        }
        (true, None, false, _) => {
            let nested_validity = std::iter::repeat(1);
            let validity = std::iter::repeat(0);
            let levels = DefLevelsIter::new(to_length(nested.offsets), nested_validity, validity);
            write_def_levels1(buffer, levels, num_bits, version)
        }
        (true, Some(nested_validity), false, _) => {
            let nested_validity = nested_validity.iter().map(|x| x as u32);
            let validity = std::iter::repeat(0);
            let levels = DefLevelsIter::new(to_length(nested.offsets), nested_validity, validity);
            write_def_levels1(buffer, levels, num_bits, version)
        }
        (false, _, true, None) => {
            let nested_validity = std::iter::repeat(0);
            let validity = std::iter::repeat(1);
            let levels = DefLevelsIter::new(to_length(nested.offsets), nested_validity, validity);
            write_def_levels1(buffer, levels, num_bits, version)
        }
        (false, _, true, Some(validity)) => {
            let nested_validity = std::iter::repeat(0);
            let validity = validity.iter().map(|x| x as u32);
            let levels = DefLevelsIter::new(to_length(nested.offsets), nested_validity, validity);
            write_def_levels1(buffer, levels, num_bits, version)
        }
        (false, _, false, _) => {
            let nested_validity = std::iter::repeat(0);
            let validity = std::iter::repeat(0);
            let levels = DefLevelsIter::new(to_length(nested.offsets), nested_validity, validity);
            write_def_levels1(buffer, levels, num_bits, version)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rep_levels() {
        let lengths = [2, 0, 3, 3, 0, 3, 0, 1].into_iter();
        let expected = vec![0u32, 1, 0, 0, 1, 1, 0, 1, 1, 0, 0, 1, 1, 0, 0];

        let result = RepLevelsIter::new(lengths).collect::<Vec<_>>();
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

        let result = DefLevelsIter::new(to_length(offsets), validity, primitive_validity)
            .collect::<Vec<_>>();
        assert_eq!(result, expected)
    }

    #[test]
    fn test_def_levels1() {
        // [[0, 1], [], [2, 0, 3], [4, 5, 6], [], [7, 8, 9], [], [10]]
        let offsets = [0, 2, 2, 5, 8, 8, 11, 11, 12].as_ref();
        let validity = std::iter::repeat(0);
        let primitive_validity = std::iter::repeat(0);
        let expected = vec![1u32, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0, 1];

        let result = DefLevelsIter::new(to_length(offsets), validity, primitive_validity)
            .collect::<Vec<_>>();
        assert_eq!(result, expected)
    }
}
