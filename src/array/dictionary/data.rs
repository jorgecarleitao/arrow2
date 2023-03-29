use crate::array::{from_data, to_data, DictionaryArray, DictionaryKey, PrimitiveArray};
use crate::datatypes::{DataType, PhysicalType};
use arrow_data::{ArrayData, ArrayDataBuilder};

impl<K: DictionaryKey> DictionaryArray<K> {
    /// Convert this array into [`ArrayData`]
    pub fn to_data(&self) -> ArrayData {
        let keys = self.keys.to_data();
        let builder = keys
            .into_builder()
            .data_type(self.data_type.clone().into())
            .child_data(vec![to_data(self.values.as_ref())]);

        // Safety: Dictionary is valid
        unsafe { builder.build_unchecked() }
    }

    /// Create this array from [`ArrayData`]
    pub fn from_data(data: &ArrayData) -> Self {
        let key = match data.data_type() {
            arrow_schema::DataType::Dictionary(k, _) => k.as_ref(),
            d => panic!("unsupported dictionary type {d}"),
        };

        let data_type = DataType::from(data.data_type().clone());
        assert_eq!(
            data_type.to_physical_type(),
            PhysicalType::Dictionary(K::KEY_TYPE)
        );

        let key_builder = ArrayDataBuilder::new(key.clone())
            .buffers(vec![data.buffers()[0].clone()])
            .nulls(data.nulls().cloned());

        // Safety: Dictionary is valid
        let key_data = unsafe { key_builder.build_unchecked() };
        let keys = PrimitiveArray::from_data(&key_data);
        let values = from_data(&data.child_data()[0]);

        Self {
            data_type,
            keys,
            values,
        }
    }
}
