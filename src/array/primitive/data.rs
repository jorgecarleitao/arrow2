use crate::array::PrimitiveArray;
use crate::types::NativeType;
use arrow_data::ArrayData;

impl<T: NativeType> PrimitiveArray<T> {
    /// Convert this array into [`ArrayData`]
    pub fn to_data(&self) -> ArrayData {
        todo!()
    }

    /// Create this array from [`ArrayData`]
    pub fn from_data(data: &ArrayData) -> Self {
        todo!()
    }
}
