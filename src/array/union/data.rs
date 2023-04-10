use crate::array::{from_data, to_data, Arrow2Arrow, UnionArray};
use crate::datatypes::DataType;
use arrow_data::{ArrayData, ArrayDataBuilder};

impl Arrow2Arrow for UnionArray {
    fn to_data(&self) -> ArrayData {
        let data_type = arrow_schema::DataType::from(self.data_type.clone());
        let mut builder = ArrayDataBuilder::new(data_type)
            .len(self.len())
            .add_buffer(self.types.clone().into())
            .child_data(self.fields.iter().map(|x| to_data(x.as_ref())).collect());

        if let Some(o) = self.offsets.clone() {
            builder = builder.add_buffer(o.into());
        }

        // Safety: Array is valid
        unsafe { builder.build_unchecked() }
    }

    fn from_data(data: &ArrayData) -> Self {
        let data_type: DataType = data.data_type().clone().into();

        let fields = data.child_data().iter().map(|d| from_data(d)).collect();
        let buffers = data.buffers();
        let types = buffers[0].clone().into();
        let offsets = (buffers.len() == 2).then(|| buffers[1].clone().into());

        // Map from type id to array index
        let map = match &data_type {
            DataType::Union(_, Some(ids), _) => {
                let mut map = [0; 127];
                for (pos, &id) in ids.iter().enumerate() {
                    map[id as usize] = pos;
                }
                Some(map)
            }
            DataType::Union(_, None, _) => None,
            _ => unreachable!("must be Union type"),
        };

        Self {
            types,
            map,
            fields,
            offsets,
            data_type,
            offset: data.offset(),
        }
    }
}
