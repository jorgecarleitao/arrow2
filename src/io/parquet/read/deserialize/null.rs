use std::sync::Arc;

use crate::{
    array::{Array, NullArray},
    datatypes::DataType,
};

use super::super::{ArrayIter, DataPages};

/// Converts [`DataPages`] to an [`Iterator`] of [`Array`]
pub fn iter_to_arrays<'a, I>(mut iter: I, data_type: DataType, chunk_size: usize) -> ArrayIter<'a>
where
    I: 'a + DataPages,
{
    let mut len = 0usize;

    while let Ok(Some(x)) = iter.next() {
        len += x.num_values()
    }

    let complete_chunks = chunk_size / len;
    let remainder = chunk_size % len;
    let i_data_type = data_type.clone();
    let complete = (0..complete_chunks).map(move |_| {
        Ok(Arc::new(NullArray::from_data(i_data_type.clone(), chunk_size)) as Arc<dyn Array>)
    });
    if len % chunk_size == 0 {
        Box::new(complete)
    } else {
        let array = NullArray::from_data(data_type, remainder);
        Box::new(complete.chain(std::iter::once(Ok(Arc::new(array) as Arc<dyn Array>))))
    }
}
