use crate::trusted_len::TrustedLen;

use std::{convert::TryInto, hint::unreachable_unchecked};

use parquet2::types::NativeType;

pub struct ExactChunksIter<'a, T: NativeType> {
    chunks: std::slice::ChunksExact<'a, u8>,
    phantom: std::marker::PhantomData<T>,
}

impl<'a, T: NativeType> ExactChunksIter<'a, T> {
    #[inline]
    pub fn new(slice: &'a [u8]) -> Self {
        assert_eq!(slice.len() % std::mem::size_of::<T>(), 0);
        let chunks = slice.chunks_exact(std::mem::size_of::<T>());
        Self {
            chunks,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, T: NativeType> Iterator for ExactChunksIter<'a, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.chunks.next().map(|chunk| {
            let chunk: <T as NativeType>::Bytes = match chunk.try_into() {
                Ok(v) => v,
                Err(_) => unsafe { unreachable_unchecked() },
            };
            T::from_le_bytes(chunk)
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.chunks.size_hint()
    }
}

unsafe impl<'a, T: NativeType> TrustedLen for ExactChunksIter<'a, T> {}
