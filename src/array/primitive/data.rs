use crate::array::{Arrow2Arrow, PrimitiveArray};
use crate::types::NativeType;
use arrow_data::ArrayData;

impl<T: NativeType> Arrow2Arrow for PrimitiveArray<T> {
    fn to_data(&self) -> ArrayData {
        todo!()
    }

    fn from_data(data: &ArrayData) -> Self {
        todo!()
    }
}
