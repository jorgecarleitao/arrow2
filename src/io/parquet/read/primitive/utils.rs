use std::convert::TryInto;

use parquet2::types::NativeType;

#[inline]
pub fn read_item<T: NativeType>(chunk: &[u8]) -> T {
    let chunk: <T as NativeType>::Bytes = match chunk.try_into() {
        Ok(v) => v,
        Err(_) => unreachable!(),
    };
    T::from_le_bytes(chunk)
}
