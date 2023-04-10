use crate::array::{Arrow2Arrow, MapArray};
use arrow_data::ArrayData;

impl Arrow2Arrow for MapArray {
    fn to_data(&self) -> ArrayData {
        todo!()
    }

    fn from_data(data: &ArrayData) -> Self {
        todo!()
    }
}
