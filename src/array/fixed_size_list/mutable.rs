use std::sync::Arc;

use crate::{
    array::{Array, MutableArray, TryExtend, TryPush},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::{ArrowError, Result},
};

use super::FixedSizeListArray;

/// The mutable version of [`FixedSizeListArray`].
#[derive(Debug)]
pub struct MutableFixedSizeListArray<M: MutableArray> {
    data_type: DataType,
    size: usize,
    values: M,
    validity: Option<MutableBitmap>,
}

impl<M: MutableArray> From<MutableFixedSizeListArray<M>> for FixedSizeListArray {
    fn from(mut other: MutableFixedSizeListArray<M>) -> Self {
        FixedSizeListArray::from_data(
            other.data_type,
            other.values.as_arc(),
            other.validity.map(|x| x.into()),
        )
    }
}

impl<M: MutableArray> MutableFixedSizeListArray<M> {
    /// Creates a new [`MutableFixedSizeListArray`] from a [`MutableArray`] and size.
    pub fn new(values: M, size: usize) -> Self {
        let data_type = FixedSizeListArray::default_datatype(values.data_type().clone(), size);
        assert_eq!(values.len(), 0);
        Self {
            size,
            data_type,
            values,
            validity: None,
        }
    }

    /// The inner values
    pub fn values(&self) -> &M {
        &self.values
    }

    fn init_validity(&mut self) {
        let len = self.values.len() / self.size;

        let mut validity = MutableBitmap::new();
        validity.extend_constant(len, true);
        validity.set(len - 1, false);
        self.validity = Some(validity)
    }

    #[inline]
    fn try_push_valid(&mut self) -> Result<()> {
        if self.values.len() % self.size != 0 {
            return Err(ArrowError::KeyOverflowError);
        };
        if let Some(validity) = &mut self.validity {
            validity.push(true)
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self) {
        (0..self.size).for_each(|_| self.values.push_null());
        match &mut self.validity {
            Some(validity) => validity.push(false),
            None => self.init_validity(),
        }
    }
    /// Shrinks the capacity of the [`MutableFixedSizeListArray`] to fit its current length.
    pub fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit();
        if let Some(validity) = &mut self.validity {
            validity.shrink_to_fit()
        }
    }
}

impl<M: MutableArray + 'static> MutableArray for MutableFixedSizeListArray<M> {
    fn len(&self) -> usize {
        self.values.len() / self.size
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        self.validity.as_ref()
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(FixedSizeListArray::from_data(
            self.data_type.clone(),
            self.values.as_arc(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        ))
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(FixedSizeListArray::from_data(
            self.data_type.clone(),
            self.values.as_arc(),
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

    #[inline]
    fn push_null(&mut self) {
        (0..self.size).for_each(|_| {
            self.values.push_null();
        });
        if let Some(validity) = &mut self.validity {
            validity.push(false)
        } else {
            self.init_validity()
        }
    }

    fn shrink_to_fit(&mut self) {
        self.shrink_to_fit()
    }
}

impl<M, I, T> TryExtend<Option<I>> for MutableFixedSizeListArray<M>
where
    M: MutableArray + TryExtend<Option<T>>,
    I: IntoIterator<Item = Option<T>>,
{
    #[inline]
    fn try_extend<II: IntoIterator<Item = Option<I>>>(&mut self, iter: II) -> Result<()> {
        for items in iter {
            self.try_push(items)?;
        }
        Ok(())
    }
}

impl<M, I, T> TryPush<Option<I>> for MutableFixedSizeListArray<M>
where
    M: MutableArray + TryExtend<Option<T>>,
    I: IntoIterator<Item = Option<T>>,
{
    #[inline]
    fn try_push(&mut self, item: Option<I>) -> Result<()> {
        if let Some(items) = item {
            self.values.try_extend(items)?;
            self.try_push_valid()?;
        } else {
            self.push_null();
        }
        Ok(())
    }
}
