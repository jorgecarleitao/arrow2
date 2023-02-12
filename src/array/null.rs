use crate::{bitmap::Bitmap, datatypes::DataType};

use crate::{
    array::{Array, FromFfi, ToFfi},
    datatypes::PhysicalType,
    error::Error,
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
    pub fn try_new(data_type: DataType, length: usize) -> Result<Self, Error> {
        if data_type.to_physical_type() != PhysicalType::Null {
            return Err(Error::oos(
                "NullArray can only be initialized with a DataType whose physical type is Boolean",
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

    /// Returns a new empty [`NullArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        Self::new(data_type, 0)
    }

    /// Returns a new [`NullArray`].
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        Self::new(data_type, length)
    }

    impl_sliced!();
    impl_into_array!();
}

impl NullArray {
    /// Returns a slice of the [`NullArray`].
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    pub fn slice(&mut self, offset: usize, length: usize) {
        assert!(
            offset + length <= self.len(),
            "the offset of the new array cannot exceed the arrays' length"
        );
        unsafe { self.slice_unchecked(offset, length) };
    }

    /// Returns a slice of the [`NullArray`].
    /// # Safety
    /// The caller must ensure that `offset + length < self.len()`.
    pub unsafe fn slice_unchecked(&mut self, _offset: usize, length: usize) {
        self.length = length;
    }

    #[inline]
    fn len(&self) -> usize {
        self.length
    }
}

impl Array for NullArray {
    impl_common_array!();

    fn validity(&self) -> Option<&Bitmap> {
        None
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
    fn buffers(&self) -> Vec<Option<*const u8>> {
        // `None` is technically not required by the specification, but older C++ implementations require it, so leaving
        // it here for backward compatibility
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
    unsafe fn try_from_ffi(array: A) -> Result<Self, Error> {
        let data_type = array.data_type().clone();
        Self::try_new(data_type, array.array().len())
    }
}
