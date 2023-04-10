use crate::array::{Arrow2Arrow, FixedSizeListArray};
use arrow_data::ArrayData;

impl Arrow2Arrow for FixedSizeListArray {
    fn to_data(&self) -> ArrayData {
        todo!()
    }

    fn from_data(data: &ArrayData) -> Self {
        todo!()
    }
}
