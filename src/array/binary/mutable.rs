use std::{iter::FromIterator, sync::Arc};

use crate::{
    array::{Array, MutableArray, Offset, TryExtend},
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
    datatypes::DataType,
    error::{ArrowError, Result},
};

use super::BinaryArray;

/// The mutable version of [`BinaryArray`].
#[derive(Debug)]
pub struct MutableBinaryArray<O: Offset> {
    offsets: MutableBuffer<O>,
    values: MutableBuffer<u8>,
    validity: Option<MutableBitmap>,
}

impl<O: Offset> From<MutableBinaryArray<O>> for BinaryArray<O> {
    fn from(other: MutableBinaryArray<O>) -> Self {
        BinaryArray::<O>::from_data(
            other.offsets.into(),
            other.values.into(),
            other.validity.map(|x| x.into()),
        )
    }
}

impl<O: Offset> Default for MutableBinaryArray<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Offset> MutableBinaryArray<O> {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let mut offsets = MutableBuffer::<O>::with_capacity(capacity + 1);
        offsets.push(O::default());
        Self {
            offsets,
            values: MutableBuffer::<u8>::new(),
            validity: None,
        }
    }

    pub fn reserve(&mut self, additional: usize) {
        self.offsets.reserve(additional);
        if let Some(x) = self.validity.as_mut() {
            x.reserve(additional)
        }
    }

    #[inline]
    fn last_offset(&self) -> O {
        *self.offsets.last().unwrap()
    }

    pub fn try_push<T: AsRef<[u8]>>(&mut self, value: Option<T>) -> Result<()> {
        match value {
            Some(value) => {
                let bytes = value.as_ref();

                let size = O::from_usize(self.values.len() + bytes.len())
                    .ok_or(ArrowError::KeyOverflowError)?;

                self.values.extend_from_slice(bytes);

                self.offsets.push(size);

                match &mut self.validity {
                    Some(validity) => validity.push(true),
                    None => {
                        self.set_validity();
                    }
                }
            }
            None => {
                self.offsets.push(self.last_offset());
                match &mut self.validity {
                    Some(validity) => validity.push(false),
                    None => {}
                }
            }
        }
        Ok(())
    }

    pub fn push<T: AsRef<[u8]>>(&mut self, value: Option<T>) {
        self.try_push(value).unwrap()
    }

    fn try_from_iter<P: AsRef<[u8]>, I: IntoIterator<Item = Option<P>>>(iter: I) -> Result<Self> {
        let iterator = iter.into_iter();
        let (lower, _) = iterator.size_hint();
        let mut primitive = Self::with_capacity(lower);
        for item in iterator {
            primitive.try_push(item.as_ref())?
        }
        Ok(primitive)
    }

    fn set_validity(&mut self) {
        self.validity = Some(MutableBitmap::from_trusted_len_iter(
            std::iter::repeat(false)
                .take(self.len() - 1)
                .chain(std::iter::once(true)),
        ))
    }
}

impl<O: Offset> MutableArray for MutableBinaryArray<O> {
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn validity(&self) -> &Option<MutableBitmap> {
        &self.validity
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(BinaryArray::from_data(
            std::mem::take(&mut self.offsets).into(),
            std::mem::take(&mut self.values).into(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        ))
    }

    fn data_type(&self) -> &DataType {
        if O::is_large() {
            &DataType::LargeUtf8
        } else {
            &DataType::Utf8
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    #[inline]
    fn push_null(&mut self) {
        self.push::<&[u8]>(None)
    }
}

impl<O: Offset, P: AsRef<[u8]>> FromIterator<Option<P>> for MutableBinaryArray<O> {
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        Self::try_from_iter(iter).unwrap()
    }
}

impl<O: Offset, T: AsRef<[u8]>> Extend<Option<T>> for MutableBinaryArray<O> {
    fn extend<I: IntoIterator<Item = Option<T>>>(&mut self, iter: I) {
        self.try_extend(iter).unwrap();
    }
}

impl<O: Offset, T: AsRef<[u8]>> TryExtend<Option<T>> for MutableBinaryArray<O> {
    fn try_extend<I: IntoIterator<Item = Option<T>>>(&mut self, iter: I) -> Result<()> {
        let mut iter = iter.into_iter();
        self.reserve(iter.size_hint().0);
        iter.try_for_each(|x| self.try_push(x))
    }
}
