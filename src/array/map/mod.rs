use std::sync::Arc;

use crate::{
    bitmap::Bitmap,
    buffer::Buffer,
    datatypes::{DataType, Field},
    types::Index,
};

use super::{new_empty_array, specification::check_offsets, Array};

mod ffi;
mod iterator;
pub use iterator::*;

/// An array representing a (key, value), both of arbitrary logical types.
#[derive(Debug, Clone)]
pub struct MapArray {
    data_type: DataType,
    // invariant: field.len() == offsets.len() - 1
    offsets: Buffer<i32>,
    field: Arc<dyn Array>,
    // invariant: offsets.len() - 1 == Bitmap::len()
    validity: Option<Bitmap>,
    offset: usize,
}

impl MapArray {
    pub(crate) fn get_field(datatype: &DataType) -> &Field {
        if let DataType::Map(field, _) = datatype.to_logical_type() {
            field.as_ref()
        } else {
            panic!("MapArray expects `DataType::Map` logical type")
        }
    }

    /// Returns a new null [`MapArray`] of `length`.
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        let field = new_empty_array(Self::get_field(&data_type).data_type().clone()).into();
        Self::from_data(
            data_type,
            Buffer::new_zeroed(length + 1),
            field,
            Some(Bitmap::new_zeroed(length)),
        )
    }

    /// Returns a new empty [`MapArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        let field = new_empty_array(Self::get_field(&data_type).data_type().clone()).into();
        Self::from_data(data_type, Buffer::from(&[0i32]), field, None)
    }

    /// Returns a new [`MapArray`].
    /// # Panic
    /// This function panics iff:
    /// * The `data_type`'s physical type is not consistent with [`MapArray`],
    /// * The `offsets` and `field` are inconsistent
    /// * The validity is not `None` and its length is different from `offsets.len() - 1`.
    pub fn from_data(
        data_type: DataType,
        offsets: Buffer<i32>,
        field: Arc<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Self {
        check_offsets(&offsets, field.len());

        if let Some(ref validity) = validity {
            assert_eq!(offsets.len() - 1, validity.len());
        }

        if let DataType::Struct(inner) = Self::get_field(&data_type).data_type() {
            if inner.len() != 2 {
                panic!("MapArray expects its inner `Struct` to have 2 fields (keys and maps)")
            }
        } else {
            panic!("MapArray expects `DataType::Struct` as its inner logical type")
        }

        Self {
            data_type,
            field,
            offsets,
            offset: 0,
            validity,
        }
    }

    /// Returns a slice of this [`MapArray`].
    /// # Panics
    /// panics iff `offset + length >= self.len()`
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Returns a slice of this [`MapArray`].
    /// # Safety
    /// The caller must ensure that `offset + length < self.len()`.
    pub unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        let offsets = self.offsets.clone().slice_unchecked(offset, length + 1);
        let validity = self
            .validity
            .clone()
            .map(|x| x.slice_unchecked(offset, length));
        Self {
            data_type: self.data_type.clone(),
            offsets,
            field: self.field.clone(),
            validity,
            offset: self.offset + offset,
        }
    }
}

// Accessors
impl MapArray {
    /// returns the offsets
    #[inline]
    pub fn offsets(&self) -> &Buffer<i32> {
        &self.offsets
    }

    /// Returns the field (guaranteed to be a `Struct`)
    #[inline]
    pub fn field(&self) -> &Arc<dyn Array> {
        &self.field
    }

    /// Returns the element at index `i`.
    #[inline]
    pub fn value(&self, i: usize) -> Box<dyn Array> {
        let offset = self.offsets[i];
        let offset_1 = self.offsets[i + 1];
        let length = (offset_1 - offset).to_usize();

        // Safety:
        // One of the invariants of the struct
        // is that offsets are in bounds
        unsafe { self.field.slice_unchecked(offset.to_usize(), length) }
    }

    /// Returns the element at index `i`.
    /// # Safety
    /// Assumes that the `i < self.len`.
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> Box<dyn Array> {
        let offset = *self.offsets.as_ptr().add(i);
        let offset_1 = *self.offsets.as_ptr().add(i + 1);
        let length = (offset_1 - offset).to_usize();

        self.field.slice_unchecked(offset.to_usize(), length)
    }
}

impl Array for MapArray {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    #[inline]
    fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }

    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice_unchecked(offset, length))
    }

    fn with_validity(&self, _validity: Option<Bitmap>) -> Box<dyn Array> {
        Box::new(self.clone())
    }
}
