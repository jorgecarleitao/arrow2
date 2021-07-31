use std::{collections::HashMap, sync::Arc};

use crate::{
    bitmap::Bitmap,
    buffer::Buffer,
    datatypes::{DataType, Field},
    scalar::{new_scalar, Scalar},
};

use super::Array;

mod iterator;

/// A union
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
    fields_hash: HashMap<i8, Arc<dyn Array>>,
    fields: Vec<Arc<dyn Array>>,
    offsets: Option<Buffer<i32>>,
    data_type: DataType,
    offset: usize,
}

impl UnionArray {
    pub fn from_data(
        data_type: DataType,
        types: Buffer<i8>,
        fields: Vec<Arc<dyn Array>>,
        offsets: Option<Buffer<i32>>,
    ) -> Self {
        let fields_hash = if let DataType::Union(f, ids, is_sparse) = &data_type {
            let ids: Vec<i8> = ids
                .as_ref()
                .map(|x| x.iter().map(|x| *x as i8).collect())
                .unwrap_or_else(|| (0..f.len() as i8).collect());
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
            ids.into_iter().zip(fields.iter().cloned()).collect()
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

    pub fn value(&self, index: usize) -> Box<dyn Scalar> {
        let field_index = self.types()[index];
        let field = self.fields_hash[&field_index].as_ref();
        let offset = self
            .offsets()
            .as_ref()
            .map(|x| x[index] as usize)
            .unwrap_or(index);
        new_scalar(field, offset)
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
            panic!("Wrong datatype passed to Struct.")
        }
    }
}
