use crate::{array::NullArray, datatypes::DataType};

use super::super::{ArrayIter, DataPages};

/// Converts [`DataPages`] to an [`Iterator`] of [`Array`]
pub fn iter_to_arrays<'a, I>(
    mut iter: I,
    data_type: DataType,
    chunk_size: Option<usize>,
) -> ArrayIter<'a>
where
    I: 'a + DataPages,
{
    let mut len = 0usize;

    while let Ok(Some(x)) = iter.next() {
        len += x.num_values()
    }
    if len == 0 {
        return Box::new(std::iter::empty());
    }

    let chunk_size = chunk_size.unwrap_or(len);

    let complete_chunks = chunk_size / len;
    let remainder = chunk_size % len;
    let i_data_type = data_type.clone();
    let complete = (0..complete_chunks)
        .map(move |_| Ok(NullArray::new(i_data_type.clone(), chunk_size).boxed()));
    if len % chunk_size == 0 {
        Box::new(complete)
    } else {
        let array = NullArray::new(data_type, remainder);
        Box::new(complete.chain(std::iter::once(Ok(array.boxed()))))
    }
}
