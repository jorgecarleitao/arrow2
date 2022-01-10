use std::convert::TryInto;

use parquet2::types::NativeType;

use crate::trusted_len::TrustedLen;

pub fn chunks<T: NativeType>(bytes: &[u8]) -> impl TrustedLen<Item = T> + '_ {
    assert_eq!(bytes.len() % std::mem::size_of::<T>(), 0);
    let chunks = bytes.chunks_exact(std::mem::size_of::<T>());
    chunks.map(|chunk| {
        let chunk: <T as NativeType>::Bytes = match chunk.try_into() {
            Ok(v) => v,
            Err(_) => unreachable!(),
        };
        T::from_le_bytes(chunk)
    })
}
