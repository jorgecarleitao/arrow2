use std::{collections::HashMap, sync::Arc};

use crate::{
    array::{display::get_value_display, display_fmt, new_empty_array, Array},
    bitmap::Bitmap,
    buffer::Buffer,
    datatypes::{DataType, Field},
    scalar::{new_scalar, Scalar},
};

mod ffi;
mod iterator;

type FieldEntry = (usize, Arc<dyn Array>);

/// [`UnionArray`] represents an array whose each slot can contain different values.
///
// How to read a value at slot i:
// ```
// let index = self.types()[i] as usize;
// let field = self.fields()[index];
// let offset = self.offsets().map(|x| x[index]).unwrap_or(i);
// let field = field.as_any().downcast to correct type;
// let value = field.value(offset);
// ```
#[derive(Debug, Clone)]
pub struct UnionArray {
    types: Buffer<i8>,
    // None represents when there is no typeid
    fields_hash: Option<HashMap<i8, FieldEntry>>,
    fields: Vec<Arc<dyn Array>>,
    offsets: Option<Buffer<i32>>,
    data_type: DataType,
    offset: usize,
}

impl UnionArray {
    pub fn new_empty(data_type: DataType) -> Self {
        if let DataType::Union(f, _, is_sparse) = &data_type {
            let fields = f
                .iter()
                .map(|x| new_empty_array(x.data_type().clone()).into())
                .collect();

            let offsets = if *is_sparse {
                None
            } else {
                Some(Buffer::new())
            };

            Self {
                data_type,
                fields_hash: None,
                fields,
                offsets,
                types: Buffer::new(),
                offset: 0,
            }
        } else {
            panic!("Union struct must be created with the corresponding Union DataType")
        }
    }

    pub fn from_data(
        data_type: DataType,
        types: Buffer<i8>,
        fields: Vec<Arc<dyn Array>>,
        offsets: Option<Buffer<i32>>,
    ) -> Self {
        let fields_hash = if let DataType::Union(f, ids, is_sparse) = &data_type {
            if f.len() != fields.len() {
                panic!(
                    "The number of `fields` must equal the number of fields in the Union DataType"
                )
            };
            let same_data_types = f
                .iter()
                .zip(fields.iter())
                .all(|(f, array)| f.data_type() == array.data_type());
            if !same_data_types {
                panic!("All fields' datatype in the union must equal the datatypes on the fields.")
            }
            if offsets.is_none() != *is_sparse {
                panic!("Sparsness flag must equal to noness of offsets in UnionArray")
            }
            ids.as_ref().map(|ids| {
                ids.iter()
                    .map(|x| *x as i8)
                    .enumerate()
                    .zip(fields.iter().cloned())
                    .map(|((i, type_), field)| (type_, (i, field)))
                    .collect()
            })
        } else {
            panic!("Union struct must be created with the corresponding Union DataType")
        };
        // not validated:
        // * `offsets` is valid
        // * max id < fields.len()
        Self {
            data_type,
            fields_hash,
            fields,
            offsets,
            types,
            offset: 0,
        }
    }

    pub fn offsets(&self) -> &Option<Buffer<i32>> {
        &self.offsets
    }

    pub fn fields(&self) -> &Vec<Arc<dyn Array>> {
        &self.fields
    }

    pub fn types(&self) -> &Buffer<i8> {
        &self.types
    }

    #[inline]
    fn field(&self, type_: i8) -> &Arc<dyn Array> {
        self.fields_hash
            .as_ref()
            .map(|x| &x[&type_].1)
            .unwrap_or_else(|| &self.fields[type_ as usize])
    }

    #[inline]
    fn field_slot(&self, index: usize) -> usize {
        self.offsets()
            .as_ref()
            .map(|x| x[index] as usize)
            .unwrap_or(index)
    }

    /// Returns the index and slot of the field to select from `self.fields`.
    pub fn index(&self, index: usize) -> (usize, usize) {
        let type_ = self.types()[index];
        let field_index = self
            .fields_hash
            .as_ref()
            .map(|x| x[&type_].0)
            .unwrap_or_else(|| type_ as usize);
        let index = self.field_slot(index);
        (field_index, index)
    }

    /// Returns the slot `index` as a [`Scalar`].
    pub fn value(&self, index: usize) -> Box<dyn Scalar> {
        let type_ = self.types()[index];
        let field = self.field(type_);
        let index = self.field_slot(index);
        new_scalar(field.as_ref(), index)
    }

    /// Returns a slice of this [`UnionArray`].
    /// # Implementation
    /// This operation is `O(F)` where `F` is the number of fields.
    /// # Panic
    /// This function panics iff `offset + length >= self.len()`.
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            fields: self.fields.clone(),
            fields_hash: self.fields_hash.clone(),
            types: self.types.clone().slice(offset, length),
            offsets: self.offsets.clone(),
            offset: self.offset + offset,
        }
    }
}

impl Array for UnionArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn len(&self) -> usize {
        self.types.len()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn validity(&self) -> &Option<Bitmap> {
        &None
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

impl UnionArray {
    pub fn get_fields(data_type: &DataType) -> &[Field] {
        if let DataType::Union(fields, _, _) = data_type {
            fields
        } else {
            panic!("Wrong datatype passed to UnionArray.")
        }
    }

    pub fn is_sparse(data_type: &DataType) -> bool {
        if let DataType::Union(_, _, is_sparse) = data_type {
            *is_sparse
        } else {
            panic!("Wrong datatype passed to UnionArray.")
        }
    }
}

impl std::fmt::Display for UnionArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let display = get_value_display(self);
        let new_lines = false;
        let head = "UnionArray";
        let iter = self
            .iter()
            .enumerate()
            .map(|(i, x)| if x.is_valid() { Some(display(i)) } else { None });
        display_fmt(iter, head, f, new_lines)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{array::*, buffer::Buffer, datatypes::*, error::Result};

    #[test]
    fn display() -> Result<()> {
        let fields = vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ];
        let data_type = DataType::Union(fields, None, true);
        let types = Buffer::from(&[0, 0, 1]);
        let fields = vec![
            Arc::new(Int32Array::from(&[Some(1), None, Some(2)])) as Arc<dyn Array>,
            Arc::new(Utf8Array::<i32>::from(&[Some("a"), Some("b"), Some("c")])) as Arc<dyn Array>,
        ];

        let array = UnionArray::from_data(data_type, types, fields, None);

        assert_eq!(format!("{}", array), "UnionArray[1, , c]");

        Ok(())
    }
}
