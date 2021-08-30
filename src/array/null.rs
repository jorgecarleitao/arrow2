use crate::{bitmap::Bitmap, datatypes::DataType};

use super::{ffi::ToFfi, Array};

/// The concrete [`Array`] of [`DataType::Null`].
#[derive(Debug, Clone)]
pub struct NullArray {
    data_type: DataType,
    length: usize,
    offset: usize,
}

impl NullArray {
    /// Returns a new empty [`NullArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        Self::from_data(data_type, 0)
    }

    /// Returns a new [`NullArray`].
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        Self::from_data(data_type, length)
    }

    /// Returns a new [`NullArray`].
    pub fn from_data(data_type: DataType, length: usize) -> Self {
        Self {
            data_type,
            length,
            offset: 0,
        }
    }

    /// Returns a slice of the [`NullArray`].
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            length,
            offset: self.offset + offset,
        }
    }
}

impl Array for NullArray {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.length
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &DataType::Null
    }

    fn validity(&self) -> &Option<Bitmap> {
        &None
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

impl std::fmt::Display for NullArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NullArray({})", self.len())
    }
}

unsafe impl ToFfi for NullArray {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        vec![]
    }

    #[inline]
    fn offset(&self) -> usize {
        self.offset
    }
}
