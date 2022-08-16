use crate::array::Offset;

use super::super::utils::Pushable;

/// [`Pushable`] for variable length binary data.
#[derive(Debug)]
pub struct Binary<O: Offset> {
    pub offsets: Offsets<O>,
    pub values: Vec<u8>,
    pub last_offset: O,
}

#[derive(Debug)]
pub struct Offsets<O: Offset>(pub Vec<O>);

impl<O: Offset> Offsets<O> {
    #[inline]
    pub fn extend_lengths<I: Iterator<Item = usize>>(&mut self, lengths: I) {
        let mut last_offset = *self.0.last().unwrap();
        self.0.extend(lengths.map(|length| {
            last_offset += O::from_usize(length).unwrap();
            last_offset
        }));
    }
}

impl<O: Offset> Pushable<O> for Offsets<O> {
    #[inline]
    fn len(&self) -> usize {
        self.0.len() - 1
    }

    #[inline]
    fn push(&mut self, value: O) {
        self.0.push(value)
    }

    #[inline]
    fn push_null(&mut self) {
        self.0.push(*self.0.last().unwrap())
    }

    #[inline]
    fn extend_constant(&mut self, additional: usize, value: O) {
        self.0.extend_constant(additional, value)
    }
}

impl<O: Offset> Binary<O> {
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(1 + capacity);
        offsets.push(O::default());
        Self {
            offsets: Offsets(offsets),
            values: Vec::with_capacity(capacity * 24),
            last_offset: O::default(),
        }
    }

    #[inline]
    pub fn push(&mut self, v: &[u8]) {
        self.values.extend(v);
        self.last_offset += O::from_usize(v.len()).unwrap();
        self.offsets.push(self.last_offset)
    }

    #[inline]
    pub fn extend_constant(&mut self, additional: usize) {
        self.offsets.extend_constant(additional, self.last_offset);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    #[inline]
    pub fn extend_lengths<I: Iterator<Item = usize>>(&mut self, lengths: I, values: &mut &[u8]) {
        let current_offset = self.last_offset;
        self.offsets.extend_lengths(lengths);
        self.last_offset = *self.offsets.0.last().unwrap(); // guaranteed to have one
        let length = self.last_offset.to_usize() - current_offset.to_usize();
        let (consumed, remaining) = values.split_at(length);
        *values = remaining;
        self.values.extend_from_slice(consumed);
    }
}

impl<'a, O: Offset> Pushable<&'a [u8]> for Binary<O> {
    #[inline]
    fn len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn push_null(&mut self) {
        self.push(&[])
    }

    #[inline]
    fn push(&mut self, value: &[u8]) {
        self.push(value)
    }

    #[inline]
    fn extend_constant(&mut self, additional: usize, value: &[u8]) {
        assert_eq!(value.len(), 0);
        self.extend_constant(additional)
    }
}

#[derive(Debug)]
pub struct BinaryIter<'a> {
    values: &'a [u8],
}

impl<'a> BinaryIter<'a> {
    pub fn new(values: &'a [u8]) -> Self {
        Self { values }
    }
}

impl<'a> Iterator for BinaryIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.values.is_empty() {
            return None;
        }
        let (length, remaining) = self.values.split_at(4);
        let length = u32::from_le_bytes(length.try_into().unwrap()) as usize;
        let (result, remaining) = remaining.split_at(length);
        self.values = remaining;
        Some(result)
    }
}

#[derive(Debug)]
pub struct SizedBinaryIter<'a> {
    iter: BinaryIter<'a>,
    remaining: usize,
}

impl<'a> SizedBinaryIter<'a> {
    pub fn new(values: &'a [u8], size: usize) -> Self {
        let iter = BinaryIter::new(values);
        Self {
            iter,
            remaining: size,
        }
    }
}

impl<'a> Iterator for SizedBinaryIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        } else {
            self.remaining -= 1
        };
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}
