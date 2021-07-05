use std::sync::Arc;

use crate::{
    array::{Array, MutableArray, Offset, TryExtend},
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
    datatypes::{DataType, Field},
    error::{ArrowError, Result},
};

use super::ListArray;

/// The mutable version of [`ListArray`].
#[derive(Debug)]
pub struct MutableListArray<O: Offset, M: MutableArray> {
    data_type: DataType,
    offsets: MutableBuffer<O>,
    values: M,
    validity: Option<MutableBitmap>,
}

impl<O: Offset, M: MutableArray + Default> MutableListArray<O, M> {
    pub fn new() -> Self {
        let values = M::default();
        let data_type = ListArray::<O>::default_datatype(values.data_type().clone());
        Self::new_from(values, data_type, 0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let values = M::default();
        let data_type = ListArray::<O>::default_datatype(values.data_type().clone());

        let mut offsets = MutableBuffer::<O>::with_capacity(capacity + 1);
        offsets.push(O::default());
        assert_eq!(values.len(), 0);
        Self {
            data_type,
            offsets,
            values,
            validity: None,
        }
    }
}

impl<O: Offset, M: MutableArray + Default> Default for MutableListArray<O, M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Offset, M: MutableArray> From<MutableListArray<O, M>> for ListArray<O> {
    fn from(mut other: MutableListArray<O, M>) -> Self {
        ListArray::from_data(
            other.data_type,
            other.offsets.into(),
            other.values.as_arc(),
            other.validity.map(|x| x.into()),
        )
    }
}

impl<O, M, I, T> TryExtend<Option<I>> for MutableListArray<O, M>
where
    O: Offset,
    M: MutableArray + TryExtend<Option<T>>,
    I: IntoIterator<Item = Option<T>>,
{
    fn try_extend<II: IntoIterator<Item = Option<I>>>(&mut self, iter: II) -> Result<()> {
        for items in iter {
            if let Some(items) = items {
                let values = self.mut_values();
                values.try_extend(items)?;
                self.try_push_valid()?;
            } else {
                self.push_null();
            }
        }
        Ok(())
    }
}

impl<O: Offset, M: MutableArray> MutableListArray<O, M> {
    pub fn new_from(values: M, data_type: DataType, capacity: usize) -> Self {
        let mut offsets = MutableBuffer::<O>::with_capacity(capacity + 1);
        offsets.push(O::default());
        assert_eq!(values.len(), 0);
        ListArray::<O>::get_child_field(&data_type);
        Self {
            data_type,
            offsets,
            values,
            validity: None,
        }
    }

    pub fn new_with_field(values: M, name: &str, nullable: bool) -> Self {
        let field = Box::new(Field::new(name, values.data_type().clone(), nullable));
        let data_type = if O::is_large() {
            DataType::LargeList(field)
        } else {
            DataType::List(field)
        };
        Self::new_from(values, data_type, 0)
    }

    pub fn new_with_capacity(values: M, capacity: usize) -> Self {
        let data_type = ListArray::<O>::default_datatype(values.data_type().clone());
        Self::new_from(values, data_type, capacity)
    }

    pub fn try_push_valid(&mut self) -> Result<()> {
        let size = self.values.len();
        let size = O::from_usize(size).ok_or(ArrowError::KeyOverflowError)?; // todo: make this error
        assert!(size >= *self.offsets.last().unwrap());

        self.offsets.push(size);
        if let Some(validity) = &mut self.validity {
            validity.push(true)
        }
        Ok(())
    }

    fn push_null(&mut self) {
        self.offsets.push(self.last_offset());
        match &mut self.validity {
            Some(validity) => validity.push(false),
            None => self.init_validity(),
        }
    }

    pub fn mut_values(&mut self) -> &mut M {
        &mut self.values
    }

    pub fn values(&self) -> &M {
        &self.values
    }

    #[inline]
    fn last_offset(&self) -> O {
        *self.offsets.last().unwrap()
    }

    fn init_validity(&mut self) {
        self.validity = Some(MutableBitmap::from_trusted_len_iter(
            std::iter::repeat(true)
                .take(self.offsets.len() - 1 - 1)
                .chain(std::iter::once(false)),
        ))
    }

    /// Converts itself into an [`Array`].
    pub fn into_arc(self) -> Arc<dyn Array> {
        let a: ListArray<O> = self.into();
        Arc::new(a)
    }
}

impl<O: Offset, M: MutableArray + 'static> MutableArray for MutableListArray<O, M> {
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn validity(&self) -> &Option<MutableBitmap> {
        &self.validity
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(ListArray::from_data(
            self.data_type.clone(),
            std::mem::take(&mut self.offsets).into(),
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

    fn push_null(&mut self) {
        self.push_null()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        array::{primitive::MutablePrimitiveArray, PrimitiveArray},
        bitmap::Bitmap,
        buffer::Buffer,
    };

    use super::*;

    #[test]
    fn basics() {
        let data = vec![
            Some(vec![Some(1i32), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), None, Some(6)]),
        ];

        let mut array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
        array.try_extend(data).unwrap();
        let array: ListArray<i32> = array.into();

        let values = PrimitiveArray::<i32>::from_data(
            DataType::Int32,
            Buffer::from([1, 2, 3, 4, 0, 6]),
            Some(Bitmap::from([true, true, true, true, false, true])),
        );

        let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
        let expected = ListArray::<i32>::from_data(
            data_type,
            Buffer::from([0, 3, 3, 6]),
            Arc::new(values),
            Some(Bitmap::from([true, false, true])),
        );
        assert_eq!(expected, array);
    }
}
