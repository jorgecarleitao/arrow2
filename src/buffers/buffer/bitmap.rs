use std::sync::Arc;

use crate::{bits::null_count, buffers::bytes::Bytes};

#[derive(Debug)]
pub struct Bitmap {
    bytes: Arc<Bytes<u8>>,
    // both are measured in bits
    offset: usize,
    length: usize,
    null_count: usize,
}

impl Bitmap {
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
}
