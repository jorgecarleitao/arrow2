use crate::array::{Arrow2Arrow, StructArray};
use arrow_data::ArrayData;

impl Arrow2Arrow for StructArray {
    fn to_data(&self) -> ArrayData {
        todo!()
    }
    
    fn from_data(data: &ArrayData) -> Self {
        todo!()
    }
}
