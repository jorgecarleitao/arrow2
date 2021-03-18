use super::Bitmap;

/// Iterator of Option<T> from an iterator and validity.
pub struct BitmapIter<'a> {
    iter: std::slice::Iter<'a, u8>,
    current_byte: &'a u8,
    len: usize,
    index: usize,
    mask: u8,
}

impl<'a> BitmapIter<'a> {
    #[inline]
    pub fn new(bitmap: &'a Bitmap) -> Self {
        let offset = bitmap.offset();
        let len = bitmap.len();
        let bytes = &bitmap.bytes()[offset / 8..];

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
}

impl<'a> Iterator for BitmapIter<'a> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        // easily predictable in branching
        if self.index == self.len {
            return None;
        } else {
            self.index += 1;
        }
        let value = self.current_byte & self.mask != 0;
        self.mask = self.mask.rotate_left(1);
        if self.mask == 1 {
            // reached a new byte => try to fetch it from the iterator
            match self.iter.next() {
                Some(v) => self.current_byte = v,
                None => return None,
            }
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
