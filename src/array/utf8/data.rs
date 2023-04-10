use crate::array::{Arrow2Arrow, Utf8Array};
use crate::offset::Offset;
use arrow_data::ArrayData;

impl<O: Offset> Arrow2Arrow for Utf8Array<O> {
    fn to_data(&self) -> ArrayData {
        todo!()
    }

    fn from_data(data: &ArrayData) -> Self {
        todo!()
    }
}
