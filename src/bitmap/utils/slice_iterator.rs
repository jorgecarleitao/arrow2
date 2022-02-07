use crate::bitmap::Bitmap;

/// Internal state of [`SlicesIterator`]
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
    max_len: usize,
    current_byte: u8,
    current_mask: u8, // starts all 1, it is shifted right (e.g. may end up as 00011111)
    state: State,
    len: usize,
    start: usize,
    on_region: bool,
}

impl<'a> SlicesIterator<'a> {
    /// Creates a new [`SlicesIterator`]
    pub fn new(values: &'a Bitmap) -> Self {
        let (buffer, offset, length) = values.as_slice();
        let mut iter = buffer.iter();

        let (current_byte, state) = match iter.next() {
            Some(b) => (*b >> offset, State::Nominal),
            None => (0, State::Finished),
        };
        let current_mask = 255u8 >> offset;

        Self {
            state,
            count: values.len() - values.null_count(),
            max_len: length,
            values: iter,
            current_mask,
            current_byte,
            len: 0,   // the length of the current region
            start: 0, // the start of the current region
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

            if self.current_mask == 255 {
                match (self.on_region, self.current_byte) {
                    (true, 255) => {
                        self.len = std::cmp::min(self.max_len - self.start, self.len + 8);
                        if let Some(&v) = self.values.next() {
                            self.current_byte = v;
                        };
                        continue;
                    }
                    (false, 0) => {
                        self.len = std::cmp::min(self.max_len - self.start, self.len + 8);
                        if let Some(&v) = self.values.next() {
                            self.current_byte = v;
                        };
                        continue;
                    }
                    (false, 255) => {
                        self.on_region = true;
                        self.start += self.len;
                        self.len = 8;
                        if let Some(&v) = self.values.next() {
                            self.current_byte = v;
                        };
                        continue;
                    }
                    (true, 0) => {
                        self.on_region = false;
                        let result = (self.start, self.len);
                        self.start += self.len;
                        self.len = 8;
                        if let Some(&v) = self.values.next() {
                            self.current_byte = v;
                        };
                        return Some(result);
                    }
                    _ => (),
                }
            }

            let trailing_zeros = (self.current_byte | !self.current_mask).trailing_zeros();
            let trailing_ones = (self.current_byte & self.current_mask).trailing_ones();

            let is_valid = trailing_ones > 0;

            match (self.on_region, is_valid) {
                (true, true) => {
                    self.len =
                        std::cmp::min(self.max_len - self.start, self.len + trailing_ones as usize);
                    self.current_byte >>= trailing_ones;
                    self.current_mask >>= trailing_ones;
                }
                (false, false) => {
                    self.len = std::cmp::min(
                        self.max_len - self.start,
                        self.len + trailing_zeros as usize,
                    );
                    self.current_byte >>= trailing_zeros;
                    self.current_mask >>= trailing_zeros;
                }
                (true, false) => {
                    self.on_region = false;
                    let result = (self.start, self.len);
                    self.start += self.len;
                    self.len = std::cmp::min(self.max_len - self.start, trailing_zeros as usize);

                    if (self.current_mask >> trailing_zeros) == 0 {
                        // reached a new byte => try to fetch it from the iterator
                        match self.values.next() {
                            Some(&v) => {
                                self.current_mask = 255u8;
                                self.current_byte = v;
                            }
                            None => self.state = State::Finished,
                        };
                    } else {
                        self.current_byte >>= trailing_zeros;
                        self.current_mask >>= trailing_zeros;
                    }
                    return Some(result);
                }
                (false, true) => {
                    self.on_region = true;
                    self.start += self.len;
                    self.len = std::cmp::min(self.max_len - self.start, trailing_ones as usize);
                    self.current_byte >>= trailing_ones;
                    self.current_mask >>= trailing_ones;
                }
            }

            if self.current_mask == 0 {
                // reached a new byte => try to fetch it from the iterator
                match self.values.next() {
                    Some(&v) => {
                        self.current_byte = v;
                        self.current_mask = 255u8;
                    }
                    None => return self.finish(),
                };
            }
        }
    }
}
