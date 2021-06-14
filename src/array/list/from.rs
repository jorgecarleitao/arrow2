use std::{iter::FromIterator, sync::Arc};

use crate::{
    array::{Array, Builder, IntoArray, Offset, ToArray, TryFromIterator},
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
    datatypes::DataType,
    error::{ArrowError, Result},
};

use super::ListArray;

#[derive(Debug)]
pub struct ListPrimitive<O: Offset, B: Builder<T>, T> {
    offsets: MutableBuffer<O>,
    values: B,
    validity: MutableBitmap,
    length: O,
    phantom: std::marker::PhantomData<T>,
}

impl<O, T, B> ListPrimitive<O, B, T>
where
    O: Offset,
    B: Builder<T>,
{
    /// Initializes a new [`ListPrimitive`] with a pre-allocated number of slots.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacities(capacity, 0)
    }

    /// Initializes a new [`ListPrimitive`] with a pre-allocated capacity of slots and values.
    pub fn with_capacities(capacity: usize, values: usize) -> Self {
        let mut offsets = MutableBuffer::<O>::with_capacity(capacity + 1);
        let length = O::default();
        offsets.push(length);

        Self {
            offsets,
            values: B::with_capacity(values),
            validity: MutableBitmap::with_capacity(capacity),
            length,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<O: Offset, A: Builder<T> + ToArray, T> ListPrimitive<O, A, T> {
    pub fn to(self, data_type: DataType) -> ListArray<O> {
        let values = self
            .values
            .to_arc(ListArray::<O>::get_child_type(&data_type));
        ListArray::from_data(data_type, self.offsets.into(), values, self.validity.into())
    }
}

impl<O: Offset, A: Builder<T> + IntoArray, T> From<ListPrimitive<O, A, T>> for ListArray<O> {
    fn from(primitive: ListPrimitive<O, A, T>) -> Self {
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

impl<O, B, T, P> FromIterator<Option<P>> for ListPrimitive<O, B, T>
where
    O: Offset,
    B: Builder<T>,
    P: IntoIterator<Item = Option<T>>,
{
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        Self::try_from_iter(iter.into_iter().map(Ok)).unwrap()
    }
}

impl<O, B, T, P> TryFromIterator<Option<P>> for ListPrimitive<O, B, T>
where
    O: Offset,
    B: Builder<T>,
    P: IntoIterator<Item = Option<T>>,
{
    fn try_from_iter<I: IntoIterator<Item = Result<Option<P>>>>(iter: I) -> Result<Self> {
        let iterator = iter.into_iter();
        let (lower, _) = iterator.size_hint();
        let mut primitive: ListPrimitive<O, B, T> = Builder::<P>::with_capacity(lower);
        for item in iterator {
            primitive.try_push(item?)?;
        }
        Ok(primitive)
    }
}

impl<O, T, B, P> Builder<P> for ListPrimitive<O, B, T>
where
    O: Offset,
    B: Builder<T>,
    P: IntoIterator<Item = Option<T>>,
{
    fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    #[inline]
    fn try_push(&mut self, value: Option<P>) -> Result<()> {
        match value {
            Some(v) => {
                v.into_iter().try_for_each(|item| {
                    self.length = self
                        .length
                        .checked_add(&O::one())
                        .ok_or(ArrowError::DictionaryKeyOverflowError)?;
                    self.values.try_push(item)
                })?;
                self.offsets.push(self.length);
                self.validity.push(true);
            }
            None => {
                self.offsets.push(self.length);
                self.validity.push(false);
            }
        }
        Ok(())
    }

    #[inline]
    fn push(&mut self, value: Option<P>) {
        self.try_push(value).unwrap()
    }
}

impl<O: Offset, B: Builder<T> + ToArray, T> ToArray for ListPrimitive<O, B, T> {
    fn to_arc(self, data_type: &DataType) -> Arc<dyn Array> {
        Arc::new(self.to(data_type.clone()))
    }
}

impl<O: Offset, B: Builder<T> + IntoArray, T> IntoArray for ListPrimitive<O, B, T> {
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

        let a: ListPrimitive<i32, Primitive<i32>, i32> = data.into_iter().collect();
        let a = a.to(ListArray::<i32>::default_datatype(DataType::Int32));
        let a = a.value(0);
        let a = a.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();

        let expected =
            Primitive::<i32>::from(vec![Some(1i32), Some(2), Some(3)]).to(DataType::Int32);
        assert_eq!(a, &expected)
    }

    #[test]
    fn primitive_natural() {
        let data = vec![
            Some(vec![Some(1i32), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), None, Some(6)]),
        ];

        let a: ListPrimitive<i32, Primitive<i32>, i32> = data.into_iter().collect();
        let a: ListArray<i32> = a.into();
        let a = a.value(0);
        let a = a.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();

        let expected =
            Primitive::<i32>::from(vec![Some(1i32), Some(2), Some(3)]).to(DataType::Int32);
        assert_eq!(a, &expected)
    }

    #[test]
    fn primitive_utf8_natural() {
        let data = vec![Some(vec![Some("1"), Some("2"), Some("3")]), None];

        let a: ListPrimitive<i32, Utf8Primitive<i32>, &str> = data.into_iter().collect();
        let a: ListArray<i32> = a.into();
        let a = a.value(0);
        let a = a.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();

        let expected = Utf8Array::<i32>::from(&[Some("1"), Some("2"), Some("3")]);
        assert_eq!(a, &expected);
    }

    #[test]
    fn utf8_push() {
        let mut a = ListPrimitive::<i32, Utf8Primitive<i32>, _>::with_capacity(10);

        a.try_push(Some(vec![Some("a")].into_iter())).unwrap();
        let a = a.into_arc();
        assert_eq!(a.len(), 1)
    }
}
