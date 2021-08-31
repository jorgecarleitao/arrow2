use std::sync::Arc;

use crate::{
    bitmap::Bitmap,
    datatypes::{DataType, Field},
};

use super::{display_fmt, ffi::ToFfi, new_empty_array, new_null_array, Array};

mod iterator;
pub use iterator::*;
mod mutable;
pub use mutable::*;

/// The Arrow's equivalent to an immutable `Vec<Option<[T; size]>>` where `T` is an Arrow type.
/// Cloning and slicing this struct is `O(1)`.
#[derive(Debug, Clone)]
pub struct FixedSizeListArray {
    size: i32, // this is redundant with `data_type`, but useful to not have to deconstruct the data_type.
    data_type: DataType,
    values: Arc<dyn Array>,
    validity: Option<Bitmap>,
    offset: usize,
}

impl FixedSizeListArray {
    /// Returns a new empty [`FixedSizeListArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        let values =
            new_empty_array(Self::get_child_and_size(&data_type).0.data_type().clone()).into();
        Self::from_data(data_type, values, None)
    }

    /// Returns a new null [`FixedSizeListArray`].
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        let values = new_null_array(
            Self::get_child_and_size(&data_type).0.data_type().clone(),
            length,
        )
        .into();
        Self::from_data(data_type, values, Some(Bitmap::new_zeroed(length)))
    }

    /// Returns a [`FixedSizeListArray`].
    pub fn from_data(
        data_type: DataType,
        values: Arc<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Self {
        let (_, size) = Self::get_child_and_size(&data_type);

        assert_eq!(values.len() % (*size as usize), 0);

        Self {
            size: *size,
            data_type,
            values,
            validity,
            offset: 0,
        }
    }

    /// Returns a slice of this [`FixedSizeListArray`].
    /// # Implementation
    /// This operation is `O(1)`.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.clone().map(|x| x.slice(offset, length));
        let values = self
            .values
            .clone()
            .slice(offset * self.size as usize, length * self.size as usize)
            .into();
        Self {
            data_type: self.data_type.clone(),
            size: self.size,
            values,
            validity,
            offset: self.offset + offset,
        }
    }

    /// Returns the inner array.
    pub fn values(&self) -> &Arc<dyn Array> {
        &self.values
    }

    /// Returns the `Vec<T>` at position `i`.
    #[inline]
    pub fn value(&self, i: usize) -> Box<dyn Array> {
        self.values
            .slice(i * self.size as usize, self.size as usize)
    }
}

impl FixedSizeListArray {
    pub(crate) fn get_child_and_size(data_type: &DataType) -> (&Field, &i32) {
        match data_type {
            DataType::FixedSizeList(child, size) => (child.as_ref(), size),
            DataType::Extension(_, child, _) => Self::get_child_and_size(child),
            _ => panic!("Wrong DataType"),
        }
    }

    /// Returns a [`DataType`] consistent with this Array.
    pub fn default_datatype(data_type: DataType, size: usize) -> DataType {
        let field = Box::new(Field::new("item", data_type, true));
        DataType::FixedSizeList(field, size as i32)
    }
}

impl Array for FixedSizeListArray {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.values.len() / self.size as usize
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn validity(&self) -> &Option<Bitmap> {
        &self.validity
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

impl std::fmt::Display for FixedSizeListArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        display_fmt(self.iter(), "FixedSizeListArray", f, true)
    }
}

unsafe impl ToFfi for FixedSizeListArray {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        vec![self.validity.as_ref().map(|x| x.as_ptr())]
    }

    fn offset(&self) -> usize {
        self.offset
    }

    fn children(&self) -> Vec<Arc<dyn Array>> {
        vec![self.values().clone()]
    }
}
