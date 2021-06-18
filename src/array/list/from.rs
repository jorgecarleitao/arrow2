use std::sync::Arc;

use crate::{
    array::{Array, Builder, IntoArray, NullableBuilder, Offset, ToArray, TryExtend},
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
    datatypes::DataType,
    error::{ArrowError, Result},
};

use super::ListArray;

#[derive(Debug)]
pub struct ListBuilder<O: Offset, B: Builder<T>, T> {
    offsets: MutableBuffer<O>,
    values: B,
    validity: MutableBitmap,
    length: O,
    phantom: std::marker::PhantomData<T>,
}

impl<O, T, B> ListBuilder<O, B, T>
where
    O: Offset,
    B: Builder<T>,
{
    /// Initializes a new [`ListBuilder`] with a pre-allocated number of slots.
    pub fn new(values: B) -> Self {
        Self::with_capacity(0, values)
    }

    /// Initializes a new [`ListBuilder`] with a pre-allocated number of slots.
    pub fn with_capacity(capacity: usize, values: B) -> Self {
        let mut offsets = MutableBuffer::<O>::with_capacity(capacity + 1);
        let length = O::default();
        offsets.push(length);

        Self {
            offsets,
            values,
            validity: MutableBitmap::with_capacity(capacity),
            length,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<O: Offset, A: Builder<T> + ToArray, T> ListBuilder<O, A, T> {
    pub fn to(self, data_type: DataType) -> ListArray<O> {
        let values = self
            .values
            .to_arc(ListArray::<O>::get_child_type(&data_type));
        ListArray::from_data(data_type, self.offsets.into(), values, self.validity.into())
    }
}

impl<O: Offset, A: Builder<T> + IntoArray, T> From<ListBuilder<O, A, T>> for ListArray<O> {
    fn from(primitive: ListBuilder<O, A, T>) -> Self {
        let values = primitive.values.into_arc();
        let data_type = ListArray::<O>::default_datatype(values.data_type().clone());
        ListArray::from_data(
            data_type,
            primitive.offsets.into(),
            values,
            primitive.validity.into(),
        )
    }
}

impl<O, T, B, P> TryExtend<Option<P>> for ListBuilder<O, B, T>
where
    O: Offset,
    B: Builder<T>,
    P: IntoIterator<Item = Option<T>>,
{
    fn try_extend<I: IntoIterator<Item = Option<P>>>(&mut self, iter: I) -> Result<()> {
        iter.into_iter().try_for_each(|item| match item {
            Some(item) => self.try_push(item),
            None => {
                self.push_null();
                Ok(())
            }
        })
    }
}

impl<O, T, B, P> Extend<Option<P>> for ListBuilder<O, B, T>
where
    O: Offset,
    B: Builder<T>,
    P: IntoIterator<Item = Option<T>>,
{
    fn extend<I: IntoIterator<Item = Option<P>>>(&mut self, iter: I) {
        self.try_extend(iter).unwrap()
    }
}

impl<O, T, B> NullableBuilder for ListBuilder<O, B, T>
where
    O: Offset,
    B: Builder<T>,
{
    #[inline]
    fn push_null(&mut self) {
        self.offsets.push(self.length);
        self.validity.push(false);
    }
}

impl<O, T, B, P> Builder<P> for ListBuilder<O, B, T>
where
    O: Offset,
    B: Builder<T>,
    P: IntoIterator<Item = Option<T>>,
{
    #[inline]
    fn try_push(&mut self, value: P) -> Result<()> {
        value.into_iter().try_for_each(|item| {
            self.length = self
                .length
                .checked_add(&O::one())
                .ok_or(ArrowError::DictionaryKeyOverflowError)?;
            match item {
                Some(item) => self.values.try_push(item),
                None => {
                    self.values.push_null();
                    Ok(())
                }
            }
        })?;
        self.offsets.push(self.length);
        self.validity.push(true);
        Ok(())
    }

    #[inline]
    fn push(&mut self, value: P) {
        self.try_push(value).unwrap()
    }
}

impl<O: Offset, B: Builder<T> + ToArray, T> ToArray for ListBuilder<O, B, T> {
    fn to_arc(self, data_type: &DataType) -> Arc<dyn Array> {
        Arc::new(self.to(data_type.clone()))
    }
}

impl<O: Offset, B: Builder<T> + IntoArray, T> IntoArray for ListBuilder<O, B, T> {
    fn into_arc(self) -> Arc<dyn Array> {
        let a: ListArray<O> = self.into();
        Arc::new(a)
    }
}

#[cfg(test)]
mod tests {
    use crate::array::*;

    use super::*;

    #[test]
    fn primitive() {
        let data = vec![
            Some(vec![Some(1i32), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), None, Some(6)]),
        ];

        let mut a = ListBuilder::<i32, _, _>::with_capacity(0, PrimitiveBuilder::<i32>::new());
        a.try_extend(data).unwrap();

        let a: ListArray<i32> = a.into();
        let a = a.value(0);
        let a = a.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();

        let expected =
            PrimitiveBuilder::<i32>::from(vec![Some(1i32), Some(2), Some(3)]).to(DataType::Int32);
        assert_eq!(a, &expected)
    }

    #[test]
    fn primitive_natural() {
        let data = vec![
            Some(vec![Some(1i32), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), None, Some(6)]),
        ];

        let mut a = ListBuilder::<i32, _, _>::with_capacity(0, PrimitiveBuilder::<i32>::new());
        a.try_extend(data).unwrap();
        let a: ListArray<i32> = a.into();
        let a = a.value(0);
        let a = a.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();

        let expected =
            PrimitiveBuilder::<i32>::from(vec![Some(1i32), Some(2), Some(3)]).to(DataType::Int32);
        assert_eq!(a, &expected)
    }

    #[test]
    fn primitive_utf8_natural() {
        let data = vec![Some(vec![Some("1"), Some("2"), Some("3")]), None];

        let mut a = ListBuilder::<i32, _, _>::with_capacity(0, Utf8Builder::<i32>::new());
        a.try_extend(data).unwrap();

        let a: ListArray<i32> = a.into();
        let a = a.value(0);
        let a = a.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();

        let expected = Utf8Array::<i32>::from(&[Some("1"), Some("2"), Some("3")]);
        assert_eq!(a, &expected);
    }

    #[test]
    fn utf8_push() {
        let mut a = ListBuilder::<i32, _, _>::with_capacity(0, Utf8Builder::<i32>::new());

        a.try_push(vec![Some("a")].into_iter()).unwrap();
        let a = a.into_arc();
        assert_eq!(a.len(), 1)
    }

    #[test]
    fn utf8_push_none() {
        let mut a = ListBuilder::<i32, _, _>::with_capacity(0, Utf8Builder::<i32>::new());

        a.push_null();
        let a = a.into_arc();
        assert_eq!(a.len(), 1)
    }
}
