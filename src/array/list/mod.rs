use std::sync::Arc;

use crate::{
    bitmap::Bitmap,
    buffer::Buffer,
    datatypes::{DataType, Field},
};

use super::{debug_fmt, new_empty_array, specification::check_offsets, Array, Offset};

mod ffi;
mod iterator;
pub use iterator::*;
mod mutable;
pub use mutable::*;

/// An [`Array`] semantically equivalent to `Vec<Option<Vec<Option<T>>>>` with Arrow's in-memory.
#[derive(Clone)]
pub struct ListArray<O: Offset> {
    data_type: DataType,
    offsets: Buffer<O>,
    values: Arc<dyn Array>,
    validity: Option<Bitmap>,
}

impl<O: Offset> ListArray<O> {
    /// Returns a new empty [`ListArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        let values = new_empty_array(Self::get_child_type(&data_type).clone()).into();
        Self::from_data(data_type, Buffer::from(vec![O::zero()]), values, None)
    }

    /// Returns a new null [`ListArray`].
    #[inline]
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        let child = Self::get_child_type(&data_type).clone();
        Self::from_data(
            data_type,
            Buffer::new_zeroed(length + 1),
            new_empty_array(child).into(),
            Some(Bitmap::new_zeroed(length)),
        )
    }

    /// Returns a new [`ListArray`].
    /// # Panic
    /// This function panics iff:
    /// * The `data_type`'s physical type is not consistent with the offset `O`.
    /// * The `offsets` and `values` are inconsistent
    /// * The validity is not `None` and its length is different from `offsets.len() - 1`.
    pub fn from_data(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Arc<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Self {
        check_offsets(&offsets, values.len());

        if let Some(ref validity) = validity {
            assert_eq!(offsets.len() - 1, validity.len());
        }

        // validate data_type
        let child_data_type = Self::get_child_type(&data_type);
        assert_eq!(
            child_data_type,
            values.data_type(),
            "The child's datatype must match the inner type of the \'data_type\'"
        );

        Self {
            data_type,
            offsets,
            values,
            validity,
        }
    }

    /// Returns a slice of this [`ListArray`].
    /// # Panics
    /// panics iff `offset + length >= self.len()`
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Returns a slice of this [`ListArray`].
    /// # Safety
    /// The caller must ensure that `offset + length < self.len()`.
    pub unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        let validity = self
            .validity
            .clone()
            .map(|x| x.slice_unchecked(offset, length));
        let offsets = self.offsets.clone().slice_unchecked(offset, length + 1);
        Self {
            data_type: self.data_type.clone(),
            offsets,
            values: self.values.clone(),
            validity,
        }
    }

    /// Sets the validity bitmap on this [`ListArray`].
    /// # Panic
    /// This function panics iff `validity.len() != self.len()`.
    pub fn with_validity(&self, validity: Option<Bitmap>) -> Self {
        if matches!(&validity, Some(bitmap) if bitmap.len() != self.len()) {
            panic!("validity should be as least as large as the array")
        }
        let mut arr = self.clone();
        arr.validity = validity;
        arr
    }
}

// Accessors
impl<O: Offset> ListArray<O> {
    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    /// Returns the element at index `i`
    #[inline]
    pub fn value(&self, i: usize) -> Box<dyn Array> {
        let offset = self.offsets[i];
        let offset_1 = self.offsets[i + 1];
        let length = (offset_1 - offset).to_usize();

        // Safety:
        // One of the invariants of the struct
        // is that offsets are in bounds
        unsafe { self.values.slice_unchecked(offset.to_usize(), length) }
    }

    /// Returns the element at index `i` as &str
    /// # Safety
    /// Assumes that the `i < self.len`.
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> Box<dyn Array> {
        let offset = *self.offsets.get_unchecked(i);
        let offset_1 = *self.offsets.get_unchecked(i + 1);
        let length = (offset_1 - offset).to_usize();

        self.values.slice_unchecked(offset.to_usize(), length)
    }

    /// The optional validity.
    #[inline]
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// The offsets [`Buffer`].
    #[inline]
    pub fn offsets(&self) -> &Buffer<O> {
        &self.offsets
    }

    /// The values.
    #[inline]
    pub fn values(&self) -> &Arc<dyn Array> {
        &self.values
    }
}

impl<O: Offset> ListArray<O> {
    /// Returns a default [`DataType`]: inner field is named "item" and is nullable
    pub fn default_datatype(data_type: DataType) -> DataType {
        let field = Box::new(Field::new("item", data_type, true));
        if O::is_large() {
            DataType::LargeList(field)
        } else {
            DataType::List(field)
        }
    }

    /// Returns a the inner [`Field`]
    /// # Panics
    /// Panics iff the logical type is not consistent with this struct.
    pub fn get_child_field(data_type: &DataType) -> &Field {
        if O::is_large() {
            match data_type.to_logical_type() {
                DataType::LargeList(child) => child.as_ref(),
                _ => panic!("ListArray<i64> expects DataType::List or DataType::LargeList"),
            }
        } else {
            match data_type.to_logical_type() {
                DataType::List(child) => child.as_ref(),
                _ => panic!("ListArray<i32> expects DataType::List or DataType::List"),
            }
        }
    }

    /// Returns a the inner [`DataType`]
    /// # Panics
    /// Panics iff the logical type is not consistent with this struct.
    pub fn get_child_type(data_type: &DataType) -> &DataType {
        Self::get_child_field(data_type).data_type()
    }
}

impl<O: Offset> Array for ListArray<O> {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
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
    fn with_validity(&self, validity: Option<Bitmap>) -> Box<dyn Array> {
        Box::new(self.with_validity(validity))
    }
}

impl<O: Offset> std::fmt::Debug for ListArray<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let head = if O::is_large() {
            "LargeListArray"
        } else {
            "ListArray"
        };
        debug_fmt(self.iter(), head, f, true)
    }
}
