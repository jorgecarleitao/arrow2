use crate::array::{Arrow2Arrow, UnionArray};
use arrow_data::ArrayData;

impl Arrow2Arrow for UnionArray {
    fn to_data(&self) -> ArrayData {
        todo!()
    }

    fn from_data(data: &ArrayData) -> Self {
        todo!()
    }
}
