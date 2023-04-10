use crate::array::{Arrow2Arrow, FixedSizeBinaryArray};
use arrow_data::ArrayData;

impl Arrow2Arrow for FixedSizeBinaryArray {
    fn to_data(&self) -> ArrayData {
        todo!()
    }

    fn from_data(data: &ArrayData) -> Self {
        todo!()
    }
}
