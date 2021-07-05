use parquet2::read::StreamingIterator;

use super::{ColumnChunkMetaData, Page};
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
    E: Clone,
    I: StreamingIterator<Item = std::result::Result<Page, E>>,
{
    let capacity = metadata.num_values() as usize;
    let mut values = MutableBuffer::<u8>::with_capacity(0);
    let mut offsets = MutableBuffer::<O>::with_capacity(1 + capacity);
    offsets.push(O::default());
    let mut validity = MutableBitmap::with_capacity(capacity);
    while let Some(page) = iter.next() {
        extend_from_page(
            page.as_ref().map_err(|x| x.clone())?,
            metadata.descriptor(),
            &mut offsets,
            &mut values,
            &mut validity,
        )?
    }

    Ok(BinaryArray::from_data(
        offsets.into(),
        values.into(),
        validity.into(),
    ))
}
