use crate::{bitmap::Bitmap, datatypes::DataType};

use crate::{
    array::{Array, FromFfi, ToFfi},
    datatypes::PhysicalType,
    error::ArrowError,
    ffi,
};

/// The concrete [`Array`] of [`DataType::Null`].
#[derive(Clone)]
pub struct NullArray {
    data_type: DataType,
    length: usize,
}

impl NullArray {
    /// Returns a new [`NullArray`].
    /// # Errors
    /// This function errors iff:
    /// * The `data_type`'s [`crate::datatypes::PhysicalType`] is not equal to [`crate::datatypes::PhysicalType::Null`].
    pub fn try_new(data_type: DataType, length: usize) -> Result<Self, ArrowError> {
        if data_type.to_physical_type() != PhysicalType::Null {
            return Err(ArrowError::oos(
                "BooleanArray can only be initialized with a DataType whose physical type is Boolean",
            ));
        }

        Ok(Self { data_type, length })
    }

    /// Returns a new [`NullArray`].
    /// # Panics
    /// This function errors iff:
    /// * The `data_type`'s [`crate::datatypes::PhysicalType`] is not equal to [`crate::datatypes::PhysicalType::Null`].
    pub fn new(data_type: DataType, length: usize) -> Self {
        Self::try_new(data_type, length).unwrap()
    }

    /// Alias for `new`
    pub fn from_data(data_type: DataType, length: usize) -> Self {
        Self::new(data_type, length)
    }

    /// Returns a new empty [`NullArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        Self::new(data_type, 0)
    }

    /// Returns a new [`NullArray`].
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        Self::new(data_type, length)
    }
}

impl NullArray {
    /// Returns a slice of the [`NullArray`].
    pub fn slice(&self, _offset: usize, length: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            length,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.length
    }
}

impl Array for NullArray {
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
        &DataType::Null
    }

    fn validity(&self) -> Option<&Bitmap> {
        None
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }

    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }

    fn with_validity(&self, _: Option<Bitmap>) -> Box<dyn Array> {
        panic!("cannot set validity of a null array")
    }
}

impl std::fmt::Debug for NullArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NullArray({})", self.len())
    }
}

unsafe impl ToFfi for NullArray {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        vec![None]
    }

    fn offset(&self) -> Option<usize> {
        Some(0)
    }

    fn to_ffi_aligned(&self) -> Self {
        self.clone()
    }
}

impl<A: ffi::ArrowArrayRef> FromFfi<A> for NullArray {
    unsafe fn try_from_ffi(array: A) -> Result<Self, ArrowError> {
        let data_type = array.data_type().clone();
        Self::try_new(data_type, array.array().len())
    }
}
