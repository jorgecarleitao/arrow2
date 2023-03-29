use crate::array::FixedSizeBinaryArray;
use arrow_data::ArrayData;

impl FixedSizeBinaryArray {
    /// Convert this array into [`ArrayData`]
    pub fn to_data(&self) -> ArrayData {
        todo!()
    }

    /// Create this array from [`ArrayData`]
    pub fn from_data(data: &ArrayData) -> Self {
        todo!()
    }
}
