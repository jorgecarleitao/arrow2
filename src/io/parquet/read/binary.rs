use super::{ColumnChunkMetaData, CompressedPage};
use crate::{
    array::{BinaryArray, Offset},
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
    error::{ArrowError, Result},
};

use super::utf8::*;

pub fn iter_to_array<O, I, E>(mut iter: I, metadata: &ColumnChunkMetaData) -> Result<BinaryArray<O>>
where
    ArrowError: From<E>,
    O: Offset,
    I: Iterator<Item = std::result::Result<CompressedPage, E>>,
{
    let capacity = metadata.num_values() as usize;
    let mut values = MutableBuffer::<u8>::with_capacity(0);
    let mut offsets = MutableBuffer::<O>::with_capacity(1 + capacity);
    offsets.push(O::default());
    let mut validity = MutableBitmap::with_capacity(capacity);
    iter.try_for_each(|page| {
        extend_from_page(
            page?,
            metadata.descriptor(),
            &mut offsets,
            &mut values,
            &mut validity,
        )
    })?;

    Ok(BinaryArray::from_data(
        offsets.into(),
        values.into(),
        validity.into(),
    ))
}
