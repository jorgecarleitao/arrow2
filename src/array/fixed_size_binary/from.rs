use std::{iter::FromIterator, sync::Arc};

use super::FixedSizeBinaryArray;
use crate::array::{Array, Builder, ToArray, TryFromIterator};
use crate::buffer::{MutableBitmap, MutableBuffer};
use crate::{datatypes::DataType, error::Result as ArrowResult};

/// auxiliary struct used to create a [`BinaryArray`] out of an iterator
#[derive(Debug)]
pub struct FixedSizeBinaryPrimitive {
    values: MutableBuffer<u8>,
    validity: MutableBitmap,
}

impl<P: AsRef<[u8]>> FromIterator<Option<P>> for FixedSizeBinaryPrimitive {
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        Self::try_from_iter(iter.into_iter().map(|x| Ok(x))).unwrap()
    }
}

impl<P> TryFromIterator<Option<P>> for FixedSizeBinaryPrimitive
where
    P: AsRef<[u8]>,
{
    fn try_from_iter<I: IntoIterator<Item = ArrowResult<Option<P>>>>(iter: I) -> ArrowResult<Self> {
        let iterator = iter.into_iter();
        let (lower, _) = iterator.size_hint();
        let mut primitive = Self::with_capacity(lower);
        for item in iterator {
            match item? {
                Some(x) => primitive.try_push(Some(&x.as_ref()))?,
                None => primitive.try_push(None)?,
            }
        }
        Ok(primitive)
    }
}

impl Builder<&[u8]> for FixedSizeBinaryPrimitive {
    #[inline]
    fn with_capacity(capacity: usize) -> Self {
        Self {
            values: MutableBuffer::<u8>::new(),
            validity: MutableBitmap::with_capacity(capacity),
        }
    }

    #[inline]
    fn try_push(&mut self, value: Option<&&[u8]>) -> ArrowResult<()> {
        match value {
            Some(v) => {
                let bytes = *v;
                self.values.extend_from_slice(bytes);
                self.validity.push(true);
            }
            None => {
                self.validity.push(false);
            }
        }
        Ok(())
    }

    #[inline]
    fn push(&mut self, value: Option<&&[u8]>) {
        self.try_push(value).unwrap()
    }
}

impl FixedSizeBinaryPrimitive {
    pub fn to(self, data_type: DataType) -> FixedSizeBinaryArray {
        FixedSizeBinaryArray::from_data(data_type, self.values.into(), self.validity.into())
    }
}

impl ToArray for FixedSizeBinaryPrimitive {
    fn to_arc(self, data_type: &DataType) -> Arc<dyn Array> {
        Arc::new(self.to(data_type.clone()))
    }
}
