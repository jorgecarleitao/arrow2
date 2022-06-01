use crate::{
    bitmap::Bitmap,
    datatypes::{DataType, Field},
    error::Error,
};

use super::{new_empty_array, new_null_array, Array};

mod ffi;
pub(super) mod fmt;
mod iterator;
pub use iterator::*;
mod mutable;
pub use mutable::*;

/// The Arrow's equivalent to an immutable `Vec<Option<[T; size]>>` where `T` is an Arrow type.
/// Cloning and slicing this struct is `O(1)`.
#[derive(Clone)]
pub struct FixedSizeListArray {
    size: usize, // this is redundant with `data_type`, but useful to not have to deconstruct the data_type.
    data_type: DataType,
    values: Box<dyn Array>,
    validity: Option<Bitmap>,
}

impl FixedSizeListArray {
    /// Creates a new [`FixedSizeListArray`].
    ///
    /// # Errors
    /// This function returns an error iff:
    /// * The `data_type`'s physical type is not [`crate::datatypes::PhysicalType::FixedSizeList`]
    /// * The `data_type`'s inner field's data type is not equal to `values.data_type`.
    /// * The length of `values` is not a multiple of `size` in `data_type`
    /// * the validity's length is not equal to `values.len() / size`.
    pub fn try_new(
        data_type: DataType,
        values: Box<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Result<Self, Error> {
        let (child, size) = Self::try_child_and_size(&data_type)?;

        let child_data_type = &child.data_type;
        let values_data_type = values.data_type();
        if child_data_type != values_data_type {
            return Err(Error::oos(
                format!("FixedSizeListArray's child's DataType must match. However, the expected DataType is {child_data_type:?} while it got {values_data_type:?}."),
            ));
        }

        if values.len() % size != 0 {
            return Err(Error::oos(format!(
                "values (of len {}) must be a multiple of size ({}) in FixedSizeListArray.",
                values.len(),
                size
            )));
        }
        let len = values.len() / size;

        if validity
            .as_ref()
            .map_or(false, |validity| validity.len() != len)
        {
            return Err(Error::oos(
                "validity mask length must be equal to the number of values divided by size",
            ));
        }

        Ok(Self {
            size,
            data_type,
            values,
            validity,
        })
    }

    /// Creates a new [`FixedSizeListArray`].
    /// # Panics
    /// This function panics iff:
    /// * The `data_type`'s physical type is not [`crate::datatypes::PhysicalType::FixedSizeList`]
    /// * The `data_type`'s inner field's data type is not equal to `values.data_type`.
    /// * The length of `values` is not a multiple of `size` in `data_type`
    /// * the validity's length is not equal to `values.len() / size`.
    pub fn new(data_type: DataType, values: Box<dyn Array>, validity: Option<Bitmap>) -> Self {
        Self::try_new(data_type, values, validity).unwrap()
    }

    /// Alias for `new`
    pub fn from_data(
        data_type: DataType,
        values: Box<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Self {
        Self::new(data_type, values, validity)
    }

    /// Returns a new empty [`FixedSizeListArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        let values = new_empty_array(Self::get_child_and_size(&data_type).0.data_type().clone());
        Self::new(data_type, values, None)
    }

    /// Returns a new null [`FixedSizeListArray`].
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        let values = new_null_array(
            Self::get_child_and_size(&data_type).0.data_type().clone(),
            length,
        );
        Self::new(data_type, values, Some(Bitmap::new_zeroed(length)))
    }

    /// Boxes self into a [`Box<dyn Array>`].
    pub fn boxed(self) -> Box<dyn Array> {
        Box::new(self)
    }

    /// Boxes self into a [`Arc<dyn Array>`].
    pub fn arced(self) -> std::sync::Arc<dyn Array> {
        std::sync::Arc::new(self)
    }
}

// must use
impl FixedSizeListArray {
    /// Returns a slice of this [`FixedSizeListArray`].
    /// # Implementation
    /// This operation is `O(1)`.
    /// # Panics
    /// panics iff `offset + length > self.len()`
    #[must_use]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Returns a slice of this [`FixedSizeListArray`].
    /// # Implementation
    /// This operation is `O(1)`.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    #[must_use]
    pub unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        let validity = self
            .validity
            .clone()
            .map(|x| x.slice_unchecked(offset, length));
        let values = self
            .values
            .clone()
            .slice_unchecked(offset * self.size as usize, length * self.size as usize);
        Self {
            data_type: self.data_type.clone(),
            size: self.size,
            values,
            validity,
        }
    }

    /// Sets the validity bitmap on this [`FixedSizeListArray`].
    /// # Panic
    /// This function panics iff `validity.len() != self.len()`.
    #[must_use]
    pub fn with_validity(&self, validity: Option<Bitmap>) -> Self {
        if matches!(&validity, Some(bitmap) if bitmap.len() != self.len()) {
            panic!("validity should be as least as large as the array")
        }
        let mut arr = self.clone();
        arr.validity = validity;
        arr
    }
}

// accessors
impl FixedSizeListArray {
    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len() / self.size as usize
    }

    /// The optional validity.
    #[inline]
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// Returns the inner array.
    pub fn values(&self) -> &Box<dyn Array> {
        &self.values
    }

    /// Returns the `Vec<T>` at position `i`.
    /// # Panic:
    /// panics iff `i >= self.len()`
    #[inline]
    pub fn value(&self, i: usize) -> Box<dyn Array> {
        self.values
            .slice(i * self.size as usize, self.size as usize)
    }

    /// Returns the `Vec<T>` at position `i`.
    /// # Safety
    /// Caller must ensure that `i < self.len()`
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> Box<dyn Array> {
        self.values
            .slice_unchecked(i * self.size as usize, self.size as usize)
    }
}

impl FixedSizeListArray {
    pub(crate) fn try_child_and_size(data_type: &DataType) -> Result<(&Field, usize), Error> {
        match data_type.to_logical_type() {
            DataType::FixedSizeList(child, size) => Ok((child.as_ref(), *size as usize)),
            _ => Err(Error::oos(
                "FixedSizeListArray expects DataType::FixedSizeList",
            )),
        }
    }

    pub(crate) fn get_child_and_size(data_type: &DataType) -> (&Field, usize) {
        Self::try_child_and_size(data_type).unwrap()
    }

    /// Returns a [`DataType`] consistent with [`FixedSizeListArray`].
    pub fn default_datatype(data_type: DataType, size: usize) -> DataType {
        let field = Box::new(Field::new("item", data_type, true));
        DataType::FixedSizeList(field, size)
    }
}

impl Array for FixedSizeListArray {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
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
    fn to_boxed(&self) -> Box<dyn Array> {
        Box::new(self.clone())
    }
}
