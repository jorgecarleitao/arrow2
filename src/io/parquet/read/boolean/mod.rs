use std::sync::Arc;

use crate::{
    array::{Array, BooleanArray},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::Result,
};

use parquet2::{metadata::ColumnDescriptor, page::DataPage};

mod basic;
mod nested;

pub use basic::stream_to_array;

use self::basic::BooleanArrayIterator;

use super::{nested_utils::Nested, DataPages};

fn page_to_array_nested(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    data_type: DataType,
    nested: &mut Vec<Box<dyn Nested>>,
    is_nullable: bool,
) -> Result<BooleanArray> {
    let capacity = page.num_values() as usize;
    let mut values = MutableBitmap::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);
    nested::extend_from_page(
        page,
        descriptor,
        is_nullable,
        nested,
        &mut values,
        &mut validity,
    )?;

    Ok(BooleanArray::from_data(
        data_type,
        values.into(),
        validity.into(),
    ))
}

/// Converts [`DataPages`] to an [`Iterator`] of [`Array`]
pub fn iter_to_arrays<'a, I: 'a>(
    iter: I,
    is_optional: bool,
    data_type: DataType,
    chunk_size: usize,
) -> Box<dyn Iterator<Item = Result<Arc<dyn Array>>> + 'a>
where
    I: DataPages,
{
    Box::new(
        BooleanArrayIterator::new(iter, data_type, chunk_size, is_optional)
            .map(|x| x.map(|x| Arc::new(x) as Arc<dyn Array>)),
    )
}
