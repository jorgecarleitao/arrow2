use std::sync::Arc;

use crate::{
    bitmap::Bitmap,
    buffer::Buffer,
    datatypes::{DataType, Field},
    error::Error,
};

use super::{
    new_empty_array,
    specification::{try_check_offsets, try_check_offsets_bounds},
    Array, Offset,
};

mod ffi;
pub(super) mod fmt;
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
    /// Creates a new [`ListArray`].
    ///
    /// # Errors
    /// This function returns an error iff:
    /// * the offsets are not monotonically increasing
    /// * The last offset is not equal to the values' length.
    /// * the validity's length is not equal to `offsets.len() - 1`.
    /// * The `data_type`'s [`crate::datatypes::PhysicalType`] is not equal to either [`crate::datatypes::PhysicalType::List`] or [`crate::datatypes::PhysicalType::LargeList`].
    /// * The `data_type`'s inner field's data type is not equal to `values.data_type`.
    /// # Implementation
    /// This function is `O(N)` - checking monotinicity is `O(N)`
    pub fn try_new(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Arc<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Result<Self, Error> {
        try_check_offsets(&offsets, values.len())?;

        if validity
            .as_ref()
            .map_or(false, |validity| validity.len() != offsets.len() - 1)
        {
            return Err(Error::oos(
                "validity mask length must match the number of values",
            ));
        }

        let child_data_type = Self::try_get_child(&data_type)?.data_type();
        let values_data_type = values.data_type();
        if child_data_type != values_data_type {
            return Err(Error::oos(
                format!("ListArray's child's DataType must match. However, the expected DataType is {child_data_type:?} while it got {values_data_type:?}."),
            ));
        }

        Ok(Self {
            data_type,
            offsets,
            values,
            validity,
        })
    }

    /// Creates a new [`ListArray`].
    ///
    /// # Panics
    /// This function panics iff:
    /// * the offsets are not monotonically increasing
    /// * The last offset is not equal to the values' length.
    /// * the validity's length is not equal to `offsets.len() - 1`.
    /// * The `data_type`'s [`crate::datatypes::PhysicalType`] is not equal to either [`crate::datatypes::PhysicalType::List`] or [`crate::datatypes::PhysicalType::LargeList`].
    /// * The `data_type`'s inner field's data type is not equal to `values.data_type`.
    /// # Implementation
    /// This function is `O(N)` - checking monotinicity is `O(N)`
    pub fn new(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Arc<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Self {
        Self::try_new(data_type, offsets, values, validity).unwrap()
    }

    /// Alias of `new`
    pub fn from_data(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Arc<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Self {
        Self::new(data_type, offsets, values, validity)
    }

    /// Returns a new empty [`ListArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        let values = new_empty_array(Self::get_child_type(&data_type).clone()).into();
        Self::new(data_type, Buffer::from(vec![O::zero()]), values, None)
    }

    /// Returns a new null [`ListArray`].
    #[inline]
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        let child = Self::get_child_type(&data_type).clone();
        Self::new(
            data_type,
            vec![O::default(); 1 + length].into(),
            new_empty_array(child).into(),
            Some(Bitmap::new_zeroed(length)),
        )
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

// unsafe construtors
impl<O: Offset> ListArray<O> {
    /// Creates a new [`ListArray`].
    ///
    /// # Errors
    /// This function returns an error iff:
    /// * The last offset is not equal to the values' length.
    /// * the validity's length is not equal to `offsets.len() - 1`.
    /// * The `data_type`'s [`crate::datatypes::PhysicalType`] is not equal to either [`crate::datatypes::PhysicalType::List`] or [`crate::datatypes::PhysicalType::LargeList`].
    /// * The `data_type`'s inner field's data type is not equal to `values.data_type`.
    /// # Safety
    /// This function is unsafe iff:
    /// * the offsets are not monotonically increasing
    /// # Implementation
    /// This function is `O(1)`
    pub unsafe fn try_new_unchecked(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Arc<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Result<Self, Error> {
        try_check_offsets_bounds(&offsets, values.len())?;

        if validity
            .as_ref()
            .map_or(false, |validity| validity.len() != offsets.len() - 1)
        {
            return Err(Error::oos(
                "validity mask length must match the number of values",
            ));
        }

        let child_data_type = Self::try_get_child(&data_type)?.data_type();
        let values_data_type = values.data_type();
        if child_data_type != values_data_type {
            return Err(Error::oos(
                format!("ListArray's child's DataType must match. However, the expected DataType is {child_data_type:?} while it got {values_data_type:?}."),
            ));
        }

        Ok(Self {
            data_type,
            offsets,
            values,
            validity,
        })
    }

    /// Creates a new [`ListArray`].
    ///
    /// # Panics
    /// This function panics iff:
    /// * The last offset is not equal to the values' length.
    /// * the validity's length is not equal to `offsets.len() - 1`.
    /// * The `data_type`'s [`crate::datatypes::PhysicalType`] is not equal to either [`crate::datatypes::PhysicalType::List`] or [`crate::datatypes::PhysicalType::LargeList`].
    /// * The `data_type`'s inner field's data type is not equal to `values.data_type`.
    /// # Safety
    /// This function is unsafe iff:
    /// * the offsets are not monotonically increasing
    /// # Implementation
    /// This function is `O(1)`
    pub unsafe fn new_unchecked(
        data_type: DataType,
        offsets: Buffer<O>,
        values: Arc<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Self {
        Self::try_new_unchecked(data_type, offsets, values, validity).unwrap()
    }
}

impl<O: Offset> ListArray<O> {
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
        if O::IS_LARGE {
            DataType::LargeList(field)
        } else {
            DataType::List(field)
        }
    }

    /// Returns a the inner [`Field`]
    /// # Panics
    /// Panics iff the logical type is not consistent with this struct.
    pub fn get_child_field(data_type: &DataType) -> &Field {
        Self::try_get_child(data_type).unwrap()
    }

    /// Returns a the inner [`Field`]
    /// # Errors
    /// Panics iff the logical type is not consistent with this struct.
    fn try_get_child(data_type: &DataType) -> Result<&Field, Error> {
        if O::IS_LARGE {
            match data_type.to_logical_type() {
                DataType::LargeList(child) => Ok(child.as_ref()),
                _ => Err(Error::oos("ListArray<i64> expects DataType::LargeList")),
            }
        } else {
            match data_type.to_logical_type() {
                DataType::List(child) => Ok(child.as_ref()),
                _ => Err(Error::oos("ListArray<i32> expects DataType::List")),
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
    fn to_boxed(&self) -> Box<dyn Array> {
        Box::new(self.clone())
    }
}
