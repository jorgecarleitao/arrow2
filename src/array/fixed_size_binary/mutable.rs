use std::sync::Arc;

use crate::{
    array::{Array, MutableArray},
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
    datatypes::DataType,
    error::{ArrowError, Result},
};

use super::FixedSizeBinaryArray;

/// Mutable version of [`FixedSizeBinaryArray`].
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
    pub fn new(size: usize) -> Self {
        Self::with_capacity(size, 0)
    }

    pub fn with_capacity(size: usize, capacity: usize) -> Self {
        Self {
            data_type: DataType::FixedSizeBinary(size as i32),
            size,
            values: MutableBuffer::<u8>::with_capacity(capacity * size),
            validity: None,
        }
    }

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

    #[inline]
    pub fn push<P: AsRef<[u8]>>(&mut self, value: Option<P>) {
        self.try_push(value).unwrap()
    }

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
        self.validity = Some(MutableBitmap::from_trusted_len_iter(
            std::iter::repeat(true)
                .take(self.len() - 1)
                .chain(std::iter::once(false)),
        ))
    }
}

impl MutableArray for MutableFixedSizeBinaryArray {
    fn len(&self) -> usize {
        self.values.len() / self.size
    }

    fn validity(&self) -> &Option<MutableBitmap> {
        &self.validity
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

#[cfg(test)]
mod tests {
    use crate::bitmap::Bitmap;

    use super::*;

    #[test]
    fn basic() {
        let array = MutableFixedSizeBinaryArray::try_from_iter(
            vec![Some(b"ab"), Some(b"bc"), None, Some(b"fh")],
            2,
        )
        .unwrap();
        assert_eq!(array.len(), 4);
    }

    #[test]
    fn push_null() {
        let mut array = MutableFixedSizeBinaryArray::new(2);
        array.push::<&[u8]>(None);

        let array: FixedSizeBinaryArray = array.into();
        assert_eq!(array.validity(), &Some(Bitmap::from([false])));
    }
}
