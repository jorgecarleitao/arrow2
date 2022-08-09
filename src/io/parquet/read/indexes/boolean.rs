use parquet2::indexes::PageIndex;

use crate::array::{BooleanArray, PrimitiveArray};

use super::ColumnIndex;

pub fn deserialize(indexes: &[PageIndex<bool>]) -> ColumnIndex {
    ColumnIndex {
        min: Box::new(BooleanArray::from_trusted_len_iter(
            indexes.iter().map(|index| index.min),
        )),
        max: Box::new(BooleanArray::from_trusted_len_iter(
            indexes.iter().map(|index| index.max),
        )),
        null_count: PrimitiveArray::from_trusted_len_iter(
            indexes
                .iter()
                .map(|index| index.null_count.map(|x| x as u64)),
        )
        .boxed(),
    }
}
