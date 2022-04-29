use parquet2::encoding::hybrid_rle::encode_u32;
use parquet2::write::Version;

use crate::{
    array::Offset,
    bitmap::{utils::BitmapIter, Bitmap},
    error::Result,
};

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

enum OffsetsIter<'a, O> {
    Optional(std::iter::Zip<std::slice::Windows<'a, O>, BitmapIter<'a>>),
    Required(std::slice::Windows<'a, O>),
}

/// Iterator adapter of parquet / dremel definition levels
pub struct DefLevelsIter<'a, O: Offset> {
    iter: OffsetsIter<'a, O>,
    primitive_validity: Option<BitmapIter<'a>>,
    remaining: usize,
    is_valid: bool,
    length: usize,
    total_size: usize,
}

impl<'a, O: Offset> DefLevelsIter<'a, O> {
    pub fn new(
        offsets: &'a [O],
        validity: Option<&'a Bitmap>,
        primitive_validity: Option<&'a Bitmap>,
    ) -> Self {
        let total_size = num_values(offsets);

        let primitive_validity = primitive_validity.map(|x| x.iter());

        let iter = validity
            .map(|x| OffsetsIter::Optional(offsets.windows(2).zip(x.iter())))
            .unwrap_or_else(|| OffsetsIter::Required(offsets.windows(2)));

        Self {
            iter,
            primitive_validity,
            remaining: 0,
            length: 0,
            is_valid: false,
            total_size,
        }
    }
}

impl<O: Offset> Iterator for DefLevelsIter<'_, O> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == self.length {
            match &mut self.iter {
                OffsetsIter::Optional(iter) => {
                    let (w, is_valid) = iter.next()?;
                    let start = w[0].to_usize();
                    let end = w[1].to_usize();
                    self.length = end - start;
                    self.remaining = 0;
                    self.is_valid = is_valid;
                    if self.length == 0 {
                        self.total_size -= 1;
                        return Some(self.is_valid as u32);
                    }
                }
                OffsetsIter::Required(iter) => {
                    let w = iter.next()?;
                    let start = w[0].to_usize();
                    let end = w[1].to_usize();
                    self.length = end - start;
                    self.remaining = 0;
                    self.is_valid = true;
                    if self.length == 0 {
                        self.total_size -= 1;
                        return Some(0);
                    }
                }
            }
        }
        self.remaining += 1;
        self.total_size -= 1;

        let (base_def, p_is_valid) = self
            .primitive_validity
            .as_mut()
            .map(|x| (1, x.next().unwrap() as u32))
            .unwrap_or((0, 0));
        let def_ = (base_def + 1) * self.is_valid as u32 + p_is_valid;
        Some(def_)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.total_size, Some(self.total_size))
    }
}

#[derive(Debug)]
pub struct NestedInfo<'a, O: Offset> {
    _is_optional: bool,
    offsets: &'a [O],
    validity: Option<&'a Bitmap>,
}

impl<'a, O: Offset> NestedInfo<'a, O> {
    pub fn new(offsets: &'a [O], validity: Option<&'a Bitmap>, is_optional: bool) -> Self {
        Self {
            _is_optional: is_optional,
            offsets,
            validity,
        }
    }

    pub fn offsets(&self) -> &'a [O] {
        self.offsets
    }
}

fn write_levels_v1<F: Fn(&mut Vec<u8>) -> Result<()>>(
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

/// writes the rep levels to a `Vec<u8>`.
pub fn write_def_levels<O: Offset>(
    buffer: &mut Vec<u8>,
    nested: &NestedInfo<O>,
    validity: Option<&Bitmap>,
    version: Version,
) -> Result<()> {
    let num_bits = 1 + validity.is_some() as u8;

    match version {
        Version::V1 => {
            write_levels_v1(buffer, |buffer: &mut Vec<u8>| {
                let levels = DefLevelsIter::new(nested.offsets, nested.validity, validity);
                encode_u32(buffer, levels, num_bits)?;
                Ok(())
            })?;
        }
        Version::V2 => {
            let levels = DefLevelsIter::new(nested.offsets, nested.validity, validity);
            encode_u32(buffer, levels, num_bits)?;
        }
    }

    Ok(())
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
        let validity = Some(Bitmap::from([
            true, false, true, true, true, true, false, true,
        ]));
        let primitive_validity = Some(Bitmap::from([
            true, true, //[0, 1]
            true, false, true, //[2, None, 3]
            true, true, true, //[4, 5, 6]
            true, true, true, //[7, 8, 9]
            true, //[10]
        ]));
        let expected = vec![3u32, 3, 0, 3, 2, 3, 3, 3, 3, 1, 3, 3, 3, 0, 3];

        let result = DefLevelsIter::new(offsets, validity.as_ref(), primitive_validity.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(result, expected)
    }

    #[test]
    fn test_def_levels1() {
        // [[0, 1], [], [2, 0, 3], [4, 5, 6], [], [7, 8, 9], [], [10]]
        let offsets = [0, 2, 2, 5, 8, 8, 11, 11, 12].as_ref();
        let validity = None;
        let primitive_validity = None;
        let expected = vec![1u32, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0, 1];

        let result = DefLevelsIter::new(offsets, validity.as_ref(), primitive_validity.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(result, expected)
    }
}
