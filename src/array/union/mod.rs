use std::{collections::HashMap, sync::Arc};

use crate::{
    bitmap::Bitmap,
    buffer::Buffer,
    datatypes::{DataType, Field, UnionMode},
    error::Error,
    scalar::{new_scalar, Scalar},
};

use super::{new_empty_array, new_null_array, Array};

mod ffi;
pub(super) mod fmt;
mod iterator;

type FieldEntry = (usize, Arc<dyn Array>);
type UnionComponents<'a> = (&'a [Field], Option<&'a [i32]>, UnionMode);

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
#[derive(Clone)]
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
    /// Returns a new [`UnionArray`].
    /// # Errors
    /// This function errors iff:
    /// * `data_type`'s physical type is not [`crate::datatypes::PhysicalType::Union`].
    /// * the fields's len is different from the `data_type`'s children's length
    /// * any of the values's data type is different from its corresponding children' data type
    pub fn try_new(
        data_type: DataType,
        types: Buffer<i8>,
        fields: Vec<Arc<dyn Array>>,
        offsets: Option<Buffer<i32>>,
    ) -> Result<Self, Error> {
        let (f, ids, mode) = Self::try_get_all(&data_type)?;

        if f.len() != fields.len() {
            return Err(Error::oos(
                "The number of `fields` must equal the number of children fields in DataType::Union",
            ));
        };

        f
            .iter().map(|a| a.data_type())
            .zip(fields.iter().map(|a| a.data_type()))
            .enumerate()
            .try_for_each(|(index, (data_type, child))| {
                if data_type != child {
                    Err(Error::oos(format!(
                        "The children DataTypes of a UnionArray must equal the children data types. 
                         However, the field {index} has data type {data_type:?} but the value has data type {child:?}"
                    )))
                } else {
                    Ok(())
                }
            })?;

        if offsets.is_none() != mode.is_sparse() {
            return Err(Error::oos(
                "The offsets must be set when the Union is dense and vice-versa",
            ));
        }

        let fields_hash = ids.as_ref().map(|ids| {
            ids.iter()
                .map(|x| *x as i8)
                .enumerate()
                .zip(fields.iter().cloned())
                .map(|((i, type_), field)| (type_, (i, field)))
                .collect()
        });

        // not validated:
        // * `offsets` is valid
        // * max id < fields.len()
        Ok(Self {
            data_type,
            fields_hash,
            fields,
            offsets,
            types,
            offset: 0,
        })
    }

    /// Returns a new [`UnionArray`].
    /// # Panics
    /// This function panics iff:
    /// * `data_type`'s physical type is not [`crate::datatypes::PhysicalType::Union`].
    /// * the fields's len is different from the `data_type`'s children's length
    /// * any of the values's data type is different from its corresponding children' data type
    pub fn new(
        data_type: DataType,
        types: Buffer<i8>,
        fields: Vec<Arc<dyn Array>>,
        offsets: Option<Buffer<i32>>,
    ) -> Self {
        Self::try_new(data_type, types, fields, offsets).unwrap()
    }

    /// Alias for `new`
    pub fn from_data(
        data_type: DataType,
        types: Buffer<i8>,
        fields: Vec<Arc<dyn Array>>,
        offsets: Option<Buffer<i32>>,
    ) -> Self {
        Self::new(data_type, types, fields, offsets)
    }

    /// Creates a new null [`UnionArray`].
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        if let DataType::Union(f, _, mode) = &data_type {
            let fields = f
                .iter()
                .map(|x| new_null_array(x.data_type().clone(), length).into())
                .collect();

            let offsets = if mode.is_sparse() {
                None
            } else {
                Some((0..length as i32).collect::<Buffer<i32>>())
            };

            // all from the same field
            let types = vec![0i8; length].into();

            Self::new(data_type, types, fields, offsets)
        } else {
            panic!("Union struct must be created with the corresponding Union DataType")
        }
    }

    /// Creates a new empty [`UnionArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        if let DataType::Union(f, _, mode) = &data_type {
            let fields = f
                .iter()
                .map(|x| new_empty_array(x.data_type().clone()).into())
                .collect();

            let offsets = if mode.is_sparse() {
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
}

impl UnionArray {
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
            offsets: self
                .offsets
                .clone()
                .map(|offsets| offsets.slice(offset, length)),
            offset: self.offset + offset,
        }
    }

    /// Returns a slice of this [`UnionArray`].
    /// # Implementation
    /// This operation is `O(F)` where `F` is the number of fields.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    #[inline]
    pub unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            fields: self.fields.clone(),
            fields_hash: self.fields_hash.clone(),
            types: self.types.clone().slice_unchecked(offset, length),
            offsets: self
                .offsets
                .clone()
                .map(|offsets| offsets.slice_unchecked(offset, length)),
            offset: self.offset + offset,
        }
    }
}

impl UnionArray {
    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.types.len()
    }

    /// The optional offsets.
    pub fn offsets(&self) -> Option<&Buffer<i32>> {
        self.offsets.as_ref()
    }

    /// The fields.
    pub fn fields(&self) -> &Vec<Arc<dyn Array>> {
        &self.fields
    }

    /// The types.
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
            .unwrap_or(index + self.offset)
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
}

impl Array for UnionArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn validity(&self) -> Option<&Bitmap> {
        None
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice_unchecked(offset, length))
    }
    fn with_validity(&self, _: Option<Bitmap>) -> Box<dyn Array> {
        panic!("cannot set validity of a union array")
    }
    fn to_boxed(&self) -> Box<dyn Array> {
        Box::new(self.clone())
    }
}

impl UnionArray {
    fn try_get_all(data_type: &DataType) -> Result<UnionComponents, Error> {
        match data_type.to_logical_type() {
            DataType::Union(fields, ids, mode) => {
                Ok((fields, ids.as_ref().map(|x| x.as_ref()), *mode))
            }
            _ => Err(Error::oos(
                "The UnionArray requires a logical type of DataType::Union",
            )),
        }
    }

    fn get_all(data_type: &DataType) -> (&[Field], Option<&[i32]>, UnionMode) {
        Self::try_get_all(data_type).unwrap()
    }

    /// Returns all fields from [`DataType::Union`].
    /// # Panic
    /// Panics iff `data_type`'s logical type is not [`DataType::Union`].
    pub fn get_fields(data_type: &DataType) -> &[Field] {
        Self::get_all(data_type).0
    }

    /// Returns whether the [`DataType::Union`] is sparse or not.
    /// # Panic
    /// Panics iff `data_type`'s logical type is not [`DataType::Union`].
    pub fn is_sparse(data_type: &DataType) -> bool {
        Self::get_all(data_type).2.is_sparse()
    }
}
