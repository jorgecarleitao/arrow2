use std::{iter::FromIterator, sync::Arc};

use super::FixedSizeBinaryArray;
use crate::array::{Array, Builder, NullableBuilder, ToArray, TryExtend, TryFromIterator};
use crate::bitmap::MutableBitmap;
use crate::buffer::MutableBuffer;
use crate::{
    datatypes::DataType,
    error::{ArrowError, Result as ArrowResult},
};

/// auxiliary struct used to create a [`BinaryArray`] out of an iterator
#[derive(Debug)]
pub struct FixedSizeBinaryBuilder {
    values: MutableBuffer<u8>,
    validity: MutableBitmap,
    size: Option<usize>,
    current_validity: usize,
}

impl FixedSizeBinaryBuilder {
    /// Initializes a new empty [`FixedSizeBinaryBuilder`].
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: MutableBuffer::<u8>::new(),
            validity: MutableBitmap::with_capacity(capacity),
            size: None,
            current_validity: 0,
        }
    }
}

impl Default for FixedSizeBinaryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl<P: AsRef<[u8]>> FromIterator<Option<P>> for FixedSizeBinaryBuilder {
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        Self::try_from_iter(iter.into_iter()).unwrap()
    }
}

impl<P> TryFromIterator<Option<P>> for FixedSizeBinaryBuilder
where
    P: AsRef<[u8]>,
{
    fn try_from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> ArrowResult<Self> {
        let mut primitive = Self::new();
        primitive.try_extend(iter)?;
        Ok(primitive)
    }
}

impl<P> TryExtend<Option<P>> for FixedSizeBinaryBuilder
where
    P: AsRef<[u8]>,
{
    fn try_extend<I: IntoIterator<Item = Option<P>>>(&mut self, iter: I) -> ArrowResult<()> {
        for item in iter {
            match item {
                Some(x) => self.try_push(x.as_ref())?,
                None => self.push_null(),
            }
        }
        Ok(())
    }
}

impl NullableBuilder for FixedSizeBinaryBuilder {
    #[inline]
    fn push_null(&mut self) {
        if let Some(size) = self.size {
            self.values
                .extend_from_trusted_len_iter(std::iter::repeat(0).take(size));
        } else {
            self.current_validity += 1;
        }
        self.validity.push(false);
    }
}

impl Builder<&[u8]> for FixedSizeBinaryBuilder {
    #[inline]
    fn try_push(&mut self, values: &[u8]) -> ArrowResult<()> {
        if let Some(size) = self.size {
            if size != values.len() {
                return Err(ArrowError::InvalidArgumentError(
                    "FixedSizeBinaryBuilder received an argument with the wrong number of items"
                        .to_string(),
                ));
            }
        } else {
            self.size = Some(values.len());
            self.values.extend_from_trusted_len_iter(
                std::iter::repeat(0).take(values.len() * self.current_validity),
            );
        };
        self.values.extend_from_slice(values);
        self.validity.push(true);
        Ok(())
    }

    #[inline]
    fn push(&mut self, value: &[u8]) {
        self.try_push(value).unwrap()
    }
}

impl FixedSizeBinaryBuilder {
    pub fn to(mut self, data_type: DataType) -> FixedSizeBinaryArray {
        let size = *FixedSizeBinaryArray::get_size(&data_type) as usize;
        if let Some(self_size) = self.size {
            assert_eq!(size, self_size);
        } else {
            self.values.extend_from_trusted_len_iter(
                std::iter::repeat(0).take(size * self.current_validity),
            )
        };

        FixedSizeBinaryArray::from_data(data_type, self.values.into(), self.validity.into())
    }
}

impl ToArray for FixedSizeBinaryBuilder {
    fn to_arc(self, data_type: &DataType) -> Arc<dyn Array> {
        Arc::new(self.to(data_type.clone()))
    }
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;

    use super::*;

    #[test]
    fn basic() {
        let array =
            FixedSizeBinaryBuilder::from_iter(vec![Some(b"ab"), Some(b"bc"), None, Some(b"fh")])
                .to(DataType::FixedSizeBinary(2));
        assert_eq!(array.len(), 4);
    }
}
