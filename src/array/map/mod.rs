use crate::{
    bitmap::Bitmap,
    buffer::Buffer,
    datatypes::{DataType, Field},
    error::Error,
};

use super::{new_empty_array, specification::try_check_offsets, Array};

mod ffi;
mod fmt;
mod iterator;
pub use iterator::*;

/// An array representing a (key, value), both of arbitrary logical types.
#[derive(Clone)]
pub struct MapArray {
    data_type: DataType,
    // invariant: field.len() == offsets.len() - 1
    offsets: Buffer<i32>,
    field: Box<dyn Array>,
    // invariant: offsets.len() - 1 == Bitmap::len()
    validity: Option<Bitmap>,
}

impl MapArray {
    /// Returns a new [`MapArray`].
    /// # Errors
    /// This function errors iff:
    /// * the offsets are not monotonically increasing
    /// * The last offset is not equal to the field' length
    /// * The `data_type`'s physical type is not [`crate::datatypes::PhysicalType::Map`]
    /// * The fields' `data_type` is not equal to the inner field of `data_type`
    /// * The validity is not `None` and its length is different from `offsets.len() - 1`.
    pub fn try_new(
        data_type: DataType,
        offsets: Buffer<i32>,
        field: Box<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Result<Self, Error> {
        try_check_offsets(&offsets, field.len())?;

        let inner_field = Self::try_get_field(&data_type)?;
        if let DataType::Struct(inner) = inner_field.data_type() {
            if inner.len() != 2 {
                return Err(Error::InvalidArgumentError(
                    "MapArray's inner `Struct` must have 2 fields (keys and maps)".to_string(),
                ));
            }
        } else {
            return Err(Error::InvalidArgumentError(
                "MapArray expects `DataType::Struct` as its inner logical type".to_string(),
            ));
        }
        if field.data_type() != inner_field.data_type() {
            return Err(Error::InvalidArgumentError(
                "MapArray expects `field.data_type` to match its inner DataType".to_string(),
            ));
        }

        if validity
            .as_ref()
            .map_or(false, |validity| validity.len() != offsets.len() - 1)
        {
            return Err(Error::oos(
                "validity mask length must match the number of values",
            ));
        }

        Ok(Self {
            data_type,
            field,
            offsets,
            validity,
        })
    }

    /// Creates a new [`MapArray`].
    /// # Panics
    /// * the offsets are not monotonically increasing
    /// * The last offset is not equal to the field' length.
    /// * The `data_type`'s physical type is not [`crate::datatypes::PhysicalType::Map`],
    /// * The validity is not `None` and its length is different from `offsets.len() - 1`.
    pub fn new(
        data_type: DataType,
        offsets: Buffer<i32>,
        field: Box<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Self {
        Self::try_new(data_type, offsets, field, validity).unwrap()
    }

    /// Alias for `new`
    pub fn from_data(
        data_type: DataType,
        offsets: Buffer<i32>,
        field: Box<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Self {
        Self::new(data_type, offsets, field, validity)
    }

    /// Returns a new null [`MapArray`] of `length`.
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        let field = new_empty_array(Self::get_field(&data_type).data_type().clone());
        Self::new(
            data_type,
            vec![0i32; 1 + length].into(),
            field,
            Some(Bitmap::new_zeroed(length)),
        )
    }

    /// Returns a new empty [`MapArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        let field = new_empty_array(Self::get_field(&data_type).data_type().clone());
        Self::new(data_type, Buffer::from(vec![0i32]), field, None)
    }

    /// Returns this [`MapArray`] with a new validity.
    /// # Panics
    /// This function panics iff `validity.len() != self.len()`.
    #[must_use]
    pub fn with_validity(mut self, validity: Option<Bitmap>) -> Self {
        self.set_validity(validity);
        self
    }

    /// Sets the validity of this [`MapArray`].
    /// # Panics
    /// This function panics iff `validity.len() != self.len()`.
    pub fn set_validity(&mut self, validity: Option<Bitmap>) {
        if matches!(&validity, Some(bitmap) if bitmap.len() != self.len()) {
            panic!("validity's length must be equal to the array's length")
        }
        self.validity = validity;
    }

    /// Boxes self into a [`Box<dyn Array>`].
    pub fn boxed(self) -> Box<dyn Array> {
        Box::new(self)
    }

    /// Boxes self into a [`std::sync::Arc<dyn Array>`].
    pub fn arced(self) -> std::sync::Arc<dyn Array> {
        std::sync::Arc::new(self)
    }
}

impl MapArray {
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
        }
    }

    pub(crate) fn try_get_field(data_type: &DataType) -> Result<&Field, Error> {
        if let DataType::Map(field, _) = data_type.to_logical_type() {
            Ok(field.as_ref())
        } else {
            Err(Error::oos(
                "The data_type's logical type must be DataType::Map",
            ))
        }
    }

    pub(crate) fn get_field(data_type: &DataType) -> &Field {
        Self::try_get_field(data_type).unwrap()
    }
}

// Accessors
impl MapArray {
    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    /// returns the offsets
    #[inline]
    pub fn offsets(&self) -> &Buffer<i32> {
        &self.offsets
    }

    /// Returns the field (guaranteed to be a `Struct`)
    #[inline]
    pub fn field(&self) -> &Box<dyn Array> {
        &self.field
    }

    /// Returns the element at index `i`.
    #[inline]
    pub fn value(&self, i: usize) -> Box<dyn Array> {
        let offset = self.offsets[i];
        let offset_1 = self.offsets[i + 1];
        let length = (offset_1 - offset) as usize;

        // Safety:
        // One of the invariants of the struct
        // is that offsets are in bounds
        unsafe { self.field.slice_unchecked(offset as usize, length) }
    }

    /// Returns the element at index `i`.
    /// # Safety
    /// Assumes that the `i < self.len`.
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> Box<dyn Array> {
        let offset = *self.offsets.get_unchecked(i);
        let offset_1 = *self.offsets.get_unchecked(i + 1);
        let length = (offset_1 - offset) as usize;

        self.field.slice_unchecked(offset as usize, length)
    }
}

impl Array for MapArray {
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
        Box::new(self.clone().with_validity(validity))
    }

    fn to_boxed(&self) -> Box<dyn Array> {
        Box::new(self.clone())
    }
}
