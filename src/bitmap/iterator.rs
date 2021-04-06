use super::Bitmap;

/// An iterator of bits according to the LSB format
pub struct BitmapIter<'a> {
    iter: std::slice::Iter<'a, u8>,
    current_byte: &'a u8,
    len: usize,
    index: usize,
    mask: u8,
}

impl<'a> BitmapIter<'a> {
    #[inline]
    pub fn new(slice: &'a [u8], offset: usize, len: usize) -> Self {
        let bytes = &slice[offset / 8..];

        let mut iter = bytes.iter();

        let current_byte = iter.next().unwrap_or(&0);

        Self {
            iter,
            mask: 1u8.rotate_left(offset as u32),
            len,
            index: 0,
            current_byte,
        }
    }

    #[inline]
    pub fn from_bitmap(bitmap: &'a Bitmap) -> Self {
        Self::new(bitmap.bytes(), bitmap.offset(), bitmap.len())
    }
}

impl<'a> Iterator for BitmapIter<'a> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.len {
            return None;
        } else {
            self.index += 1;
        }
        let value = self.current_byte & self.mask != 0;
        self.mask = self.mask.rotate_left(1);
        if self.mask == 1 {
            // reached a new byte => try to fetch it from the byte iterator
            if let Some(next_byte) = self.iter.next() {
                self.current_byte = next_byte
            }
            // no byte: we reached the end.
        }
        Some(value)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len - self.index, Some(self.len - self.index))
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let (end, overflow) = self.index.overflowing_add(n);
        if end > self.len || overflow {
            self.index = self.len;
            None
        } else {
            self.mask = self.mask.rotate_left((n % 8) as u32);

            if (self.index % 8 + n) >= 8 {
                // need to fetch the new byte.
                // infalible because self.index + n < self.len;
                self.current_byte = self.iter.nth((n / 8).saturating_sub(1)).unwrap();
            };
            let value = self.current_byte & self.mask != 0;
            self.mask = self.mask.rotate_left(1);
            self.index += n;
            Some(value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let values = &[0b01011011u8];
        let iter = BitmapIter::new(values, 0, 6);
        let result = iter.collect::<Vec<_>>();
        assert_eq!(result, vec![true, true, false, true, true, false])
    }

    #[test]
    fn large() {
        let values = &[0b01011011u8];
        let values = std::iter::repeat(values)
            .take(63)
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        let len = 63 * 8;
        let iter = BitmapIter::new(&values, 0, len);
        assert_eq!(iter.count(), len);
    }

    #[test]
    fn offset() {
        let values = &[0b01011011u8];
        let iter = BitmapIter::new(values, 2, 4);
        let result = iter.collect::<Vec<_>>();
        assert_eq!(result, vec![false, true, true, false])
    }
}
