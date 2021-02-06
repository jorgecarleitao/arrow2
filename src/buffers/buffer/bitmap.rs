use std::sync::Arc;

use crate::{
    bits::{get_bit, null_count},
    buffers::bytes::Bytes,
};

#[derive(Debug)]
pub struct Bitmap {
    bytes: Arc<Bytes<u8>>,
    // both are measured in bits. They are used to bound the bitmap to a region of Bytes.
    offset: usize,
    length: usize,
    // this is a cache: it must be computed on initialization
    null_count: usize,
}

impl Bitmap {
    #[inline]
    pub fn null_count_range(&self, offset: usize, length: usize) -> usize {
        null_count(&self.bytes, self.offset + offset, length)
    }

    #[inline]
    pub fn null_count(&self) -> usize {
        self.null_count
    }

    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let offset = self.offset + offset;
        Self {
            bytes: self.bytes.clone(),
            offset: self.offset + offset,
            length,
            null_count: null_count(&self.bytes, offset, length),
        }
    }

    #[inline]
    pub fn get_bit(&self, i: usize) -> bool {
        get_bit(&self.bytes[self.offset..self.offset + self.length], i)
    }
}
