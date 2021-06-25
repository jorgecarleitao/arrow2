use crate::bitmap::Bitmap;

/// Internal state of [SlicesIterator]
#[derive(Debug, Clone, PartialEq)]
enum State {
    // normal iteration
    Nominal,
    // nothing more to iterate.
    Finished,
}

/// Iterator over a bitmap that returns slices of set regions
/// This is the most efficient method to extract slices of values from arrays
/// with a validity bitmap.
/// For example, the bitmap `00101111` returns `[(0,4), (6,1)]`
#[derive(Debug, Clone)]
pub struct SlicesIterator<'a> {
    values: std::slice::Iter<'a, u8>,
    count: usize,
    mask: u8,
    max_len: usize,
    current_byte: &'a u8,
    state: State,
    len: usize,
    start: usize,
    on_region: bool,
}

impl<'a> SlicesIterator<'a> {
    pub fn new(values: &'a Bitmap) -> Self {
        let offset = values.offset();
        let buffer = &values.bytes()[offset / 8..];
        let mut iter = buffer.iter();

        let (current_byte, state) = match iter.next() {
            Some(b) => (b, State::Nominal),
            None => (&0, State::Finished),
        };

        Self {
            state,
            count: values.len() - values.null_count(),
            max_len: values.len(),
            values: iter,
            mask: 1u8.rotate_left(offset as u32),
            current_byte,
            len: 0,
            start: 0,
            on_region: false,
        }
    }

    fn finish(&mut self) -> Option<(usize, usize)> {
        self.state = State::Finished;
        if self.on_region {
            Some((self.start, self.len))
        } else {
            None
        }
    }

    #[inline]
    fn current_len(&self) -> usize {
        self.start + self.len
    }

    /// Returns the total number of slots.
    /// It corresponds to the sum of all lengths of all slices.
    #[inline]
    pub fn slots(&self) -> usize {
        self.count
    }
}

impl<'a> Iterator for SlicesIterator<'a> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.state == State::Finished {
                return None;
            }
            if self.current_len() == self.max_len {
                return self.finish();
            }

            if self.mask == 1 {
                // at the beginning of a byte => try to skip it all together
                match (self.on_region, self.current_byte) {
                    (true, &255u8) => {
                        self.len += 8;
                        match self.values.next() {
                            Some(v) => self.current_byte = v,
                            None => return self.finish(),
                        };
                        continue;
                    }
                    (false, &0) => {
                        self.len += 8;
                        match self.values.next() {
                            Some(v) => self.current_byte = v,
                            None => return self.finish(),
                        };
                        continue;
                    }
                    _ => (), // we need to run over all bits of this byte
                }
            };

            let value = (self.current_byte & self.mask) != 0;
            self.mask = self.mask.rotate_left(1);

            match (self.on_region, value) {
                (true, true) => self.len += 1,
                (false, false) => self.len += 1,
                (true, false) => {
                    if self.mask == 1 {
                        // reached a new byte => try to fetch it from the iterator
                        match self.values.next() {
                            Some(v) => {
                                self.on_region = false;
                                let result = (self.start, self.len);
                                self.start += self.len;
                                self.len = 1;
                                self.current_byte = v;
                                return Some(result);
                            }
                            None => return self.finish(),
                        };
                    } else {
                        self.on_region = false;
                        let result = (self.start, self.len);
                        self.start += self.len;
                        self.len = 1;
                        return Some(result);
                    }
                }
                (false, true) => {
                    self.start += self.len;
                    self.len = 1;
                    self.on_region = true;
                }
            }

            if self.mask == 1 {
                // reached a new byte => try to fetch it from the iterator
                match self.values.next() {
                    Some(v) => self.current_byte = v,
                    None => return self.finish(),
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_invariant() {
        let values = (0..8).map(|i| i % 2 != 0).collect::<Bitmap>();
        let iter = SlicesIterator::new(&values);

        let slots = iter.slots();

        let slices = iter.collect::<Vec<_>>();

        assert_eq!(slices, vec![(1, 1), (3, 1), (5, 1), (7, 1)]);

        let mut sum = 0;
        for (_, len) in slices {
            sum += len;
        }
        assert_eq!(sum, slots);
    }

    #[test]
    fn single_set() {
        let values = (0..16).map(|i| i == 1).collect::<Bitmap>();

        let iter = SlicesIterator::new(&values);
        let count = iter.slots();
        let chunks = iter.collect::<Vec<_>>();

        assert_eq!(chunks, vec![(1, 1)]);
        assert_eq!(count, 1);
    }

    #[test]
    fn single_unset() {
        let values = (0..64).map(|i| i != 1).collect::<Bitmap>();

        let iter = SlicesIterator::new(&values);
        let count = iter.slots();
        let chunks = iter.collect::<Vec<_>>();

        assert_eq!(chunks, vec![(0, 1), (2, 62)]);
        assert_eq!(count, 64 - 1);
    }

    #[test]
    fn generic() {
        let values = (0..130).map(|i| i % 62 != 0).collect::<Bitmap>();

        let iter = SlicesIterator::new(&values);
        let count = iter.slots();
        let chunks = iter.collect::<Vec<_>>();

        assert_eq!(chunks, vec![(1, 61), (63, 61), (125, 5)]);
        assert_eq!(count, 61 + 61 + 5);
    }

    #[test]
    fn incomplete_byte() {
        let values = (0..6).map(|i| i == 1).collect::<Bitmap>();

        let iter = SlicesIterator::new(&values);
        let count = iter.slots();
        let chunks = iter.collect::<Vec<_>>();

        assert_eq!(chunks, vec![(1, 1)]);
        assert_eq!(count, 1);
    }

    #[test]
    fn incomplete_byte1() {
        let values = (0..12).map(|i| i == 9).collect::<Bitmap>();

        let iter = SlicesIterator::new(&values);
        let count = iter.slots();
        let chunks = iter.collect::<Vec<_>>();

        assert_eq!(chunks, vec![(9, 1)]);
        assert_eq!(count, 1);
    }

    #[test]
    fn end_of_byte() {
        let values = (0..16).map(|i| i != 7).collect::<Bitmap>();

        let iter = SlicesIterator::new(&values);
        let count = iter.slots();
        let chunks = iter.collect::<Vec<_>>();

        assert_eq!(chunks, vec![(0, 7), (8, 8)]);
        assert_eq!(count, 15);
    }

    #[test]
    fn bla() {
        let values = vec![true, true, true, true, true, true, true, false]
            .into_iter()
            .collect::<Bitmap>();
        let iter = SlicesIterator::new(&values);
        let count = iter.slots();
        assert_eq!(values.null_count() + iter.slots(), values.len());

        let total = iter.into_iter().fold(0, |acc, x| acc + x.1);

        assert_eq!(count, total);
    }

    #[test]
    fn past_end_should_not_be_returned() {
        let values = Bitmap::from_u8_slice(&[0b11111010], 3);
        let iter = SlicesIterator::new(&values);
        let count = iter.slots();
        assert_eq!(values.null_count() + iter.slots(), values.len());

        let total = iter.into_iter().fold(0, |acc, x| acc + x.1);

        assert_eq!(count, total);
    }
}
