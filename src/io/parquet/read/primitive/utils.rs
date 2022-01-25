use std::convert::TryInto;

use crate::types::NativeType;
use parquet2::types::NativeType as ParquetNativeType;

#[inline]
pub fn read_item<T: ParquetNativeType>(chunk: &[u8]) -> T {
    let chunk: <T as ParquetNativeType>::Bytes = match chunk.try_into() {
        Ok(v) => v,
        Err(_) => unreachable!(),
    };
    T::from_le_bytes(chunk)
}

#[inline]
pub fn chunks<T: ParquetNativeType>(bytes: &[u8]) -> impl Iterator<Item = T> + '_ {
    assert_eq!(bytes.len() % std::mem::size_of::<T>(), 0);
    let chunks = bytes.chunks_exact(std::mem::size_of::<T>());
    chunks.map(read_item)
}
