use crate::array::ListArray;
use crate::offset::Offset;
use arrow_data::ArrayData;

impl<O: Offset> ListArray<O> {
    /// Convert this array into [`ArrayData`]
    pub fn to_data(&self) -> ArrayData {
        todo!()
    }

    /// Create this array from [`ArrayData`]
    pub fn from_data(data: &ArrayData) -> Self {
        todo!()
    }
}
