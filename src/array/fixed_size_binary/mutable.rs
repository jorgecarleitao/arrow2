use std::sync::Arc;

use crate::{
    array::{Array, MutableArray},
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
    datatypes::DataType,
    error::{ArrowError, Result},
};

use super::{FixedSizeBinaryArray, FixedSizeBinaryValues};

/// The Arrow's equivalent to a mutable `Vec<Option<[u8; size]>>`.
/// Converting a [`MutableFixedSizeBinaryArray`] into a [`FixedSizeBinaryArray`] is `O(1)`.
/// # Implementation
/// This struct does not allocate a validity until one is required (i.e. push a null to it).
#[derive(Debug)]
pub struct MutableFixedSizeBinaryArray {
    data_type: DataType,
    size: usize,
    values: MutableBuffer<u8>,
    validity: Option<MutableBitmap>,
}

impl From<MutableFixedSizeBinaryArray> for FixedSizeBinaryArray {
    fn from(other: MutableFixedSizeBinaryArray) -> Self {
        FixedSizeBinaryArray::from_data(
            other.data_type,
            other.values.into(),
            other.validity.map(|x| x.into()),
        )
    }
}

impl MutableFixedSizeBinaryArray {
    /// Canonical method to create a new [`MutableFixedSizeBinaryArray`].
    pub fn from_data(
        data_type: DataType,
        values: MutableBuffer<u8>,
        validity: Option<MutableBitmap>,
    ) -> Self {
        let size = *FixedSizeBinaryArray::get_size(&data_type) as usize;
        assert_eq!(
            values.len() % size,
            0,
            "The len of values must be a multiple of size"
        );
        if let Some(validity) = &validity {
            assert_eq!(
                validity.len(),
                values.len() / size,
                "The len of the validity must be equal to values / size"
            );
        }
        Self {
            data_type,
            size,
            values,
            validity,
        }
    }

    /// Creates a new empty [`MutableFixedSizeBinaryArray`].
    pub fn new(size: usize) -> Self {
        Self::with_capacity(size, 0)
    }

    /// Creates a new [`MutableFixedSizeBinaryArray`] with capacity for `capacity` entries.
    pub fn with_capacity(size: usize, capacity: usize) -> Self {
        Self::from_data(
            DataType::FixedSizeBinary(size as i32),
            MutableBuffer::<u8>::with_capacity(capacity * size),
            None,
        )
    }

    /// tries to push a new entry to [`MutableFixedSizeBinaryArray`].
    /// # Error
    /// Errors iff the size of `value` is not equal to its own size.
    #[inline]
    pub fn try_push<P: AsRef<[u8]>>(&mut self, value: Option<P>) -> Result<()> {
        match value {
            Some(bytes) => {
                let bytes = bytes.as_ref();
                if self.size != bytes.len() {
                    return Err(ArrowError::InvalidArgumentError(
                        "FixedSizeBinaryArray requires every item to be of its length".to_string(),
                    ));
                }
                self.values.extend_from_slice(bytes);

                match &mut self.validity {
                    Some(validity) => validity.push(true),
                    None => {}
                }
            }
            None => {
                self.values.extend_constant(self.size, 0);
                match &mut self.validity {
                    Some(validity) => validity.push(false),
                    None => self.init_validity(),
                }
            }
        }
        Ok(())
    }

    /// pushes a new entry to [`MutableFixedSizeBinaryArray`].
    /// # Panics
    /// Panics iff the size of `value` is not equal to its own size.
    #[inline]
    pub fn push<P: AsRef<[u8]>>(&mut self, value: Option<P>) {
        self.try_push(value).unwrap()
    }

    /// Creates a new [`MutableFixedSizeBinaryArray`] from an iterator of values.
    /// # Errors
    /// Errors iff the size of any of the `value` is not equal to its own size.
    pub fn try_from_iter<P: AsRef<[u8]>, I: IntoIterator<Item = Option<P>>>(
        iter: I,
        size: usize,
    ) -> Result<Self> {
        let iterator = iter.into_iter();
        let (lower, _) = iterator.size_hint();
        let mut primitive = Self::with_capacity(size, lower);
        for item in iterator {
            primitive.try_push(item)?
        }
        Ok(primitive)
    }

    fn init_validity(&mut self) {
        let mut validity = MutableBitmap::new();
        validity.extend_constant(self.len(), true);
        validity.set(self.len() - 1, false);
        self.validity = Some(validity)
    }

    /// Returns the element at index `i` as `&[u8]`
    #[inline]
    pub fn value(&self, i: usize) -> &[u8] {
        &self.values[i * self.size..(i + 1) * self.size]
    }

    /// Returns the element at index `i` as `&[u8]`
    /// # Safety
    /// Assumes that the `i < self.len`.
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> &[u8] {
        std::slice::from_raw_parts(self.values.as_ptr().add(i * self.size), self.size)
    }
}

/// Accessors
impl MutableFixedSizeBinaryArray {
    /// Returns its values.
    pub fn values(&self) -> &MutableBuffer<u8> {
        &self.values
    }

    /// Returns a mutable slice of values.
    pub fn values_mut_slice(&mut self) -> &mut [u8] {
        self.values.as_mut_slice()
    }
}

impl MutableArray for MutableFixedSizeBinaryArray {
    fn len(&self) -> usize {
        self.values.len() / self.size
    }

    fn validity(&self) -> &Option<MutableBitmap> {
        &self.validity
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(FixedSizeBinaryArray::from_data(
            DataType::FixedSizeBinary(self.size as i32),
            std::mem::take(&mut self.values).into(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        ))
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(FixedSizeBinaryArray::from_data(
            DataType::FixedSizeBinary(self.size as i32),
            std::mem::take(&mut self.values).into(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        ))
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn push_null(&mut self) {
        self.values.extend_constant(self.size, 0);
    }
}

impl FixedSizeBinaryValues for MutableFixedSizeBinaryArray {
    #[inline]
    fn values(&self) -> &[u8] {
        &self.values
    }

    #[inline]
    fn size(&self) -> usize {
        self.size
    }
}

impl PartialEq for MutableFixedSizeBinaryArray {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}
