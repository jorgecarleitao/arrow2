use crate::trusted_len::TrustedLen;

use super::get_bit_unchecked;

/// An iterator over bits according to the [LSB](https://en.wikipedia.org/wiki/Bit_numbering#Least_significant_bit),
/// i.e. the bytes `[4u8, 128u8]` correspond to `[false, false, true, false, ..., true]`.
pub struct BitmapIter<'a> {
    bytes: &'a [u8],
    index: usize,
    end: usize,
}

impl<'a> BitmapIter<'a> {
    #[inline]
    pub fn new(slice: &'a [u8], offset: usize, len: usize) -> Self {
        // example:
        // slice.len() = 4
        // offset = 9
        // len = 23
        // result:
        let bytes = &slice[offset / 8..];
        // bytes.len() = 3
        let index = offset % 8;
        // index = 9 % 8 = 1
        let end = len + index;
        // end = 23 + 1 = 24
        assert!(end <= bytes.len() * 8);
        // maximum read before UB in bits: bytes.len() * 8 = 24
        // the first read from the end is `end - 1`, thus, end = 24 is ok

        Self { bytes, index, end }
    }
}

impl<'a> Iterator for BitmapIter<'a> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        // See comment in `new`
        Some(unsafe { get_bit_unchecked(self.bytes, old) })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.end - self.index;
        (exact, Some(exact))
    }
}

impl<'a> DoubleEndedIterator for BitmapIter<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<bool> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            // See comment in `new`; end was first decreased
            Some(unsafe { get_bit_unchecked(self.bytes, self.end) })
        }
    }
}

unsafe impl TrustedLen for BitmapIter<'_> {}

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

    #[test]
    fn rev() {
        let values = &[0b01011011u8, 0b01011011u8];
        let iter = BitmapIter::new(values, 2, 13);
        let result = iter.rev().collect::<Vec<_>>();
        assert_eq!(
            result,
            vec![false, true, true, false, true, false, true, true, false, true, true, false, true]
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
        )
    }
}
