use std::sync::Arc;

use crate::{
    array::{Array, Builder, NullableBuilder, ToArray, TryExtend},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::Result,
};

use super::FixedSizeListArray;

#[derive(Debug)]
pub struct FixedSizeListBuilder<B: Builder<T>, T> {
    values: B,
    validity: MutableBitmap,
    phantom: std::marker::PhantomData<T>,
}

impl<B: Builder<T>, T> FixedSizeListBuilder<B, T> {
    #[inline]
    pub fn new(values: B) -> Self {
        Self::with_capacity(0, values)
    }

    #[inline]
    pub fn with_capacity(capacity: usize, values: B) -> Self {
        Self {
            values,
            validity: MutableBitmap::with_capacity(capacity),
            phantom: std::marker::PhantomData,
        }
    }
}

impl<A: Builder<T> + ToArray, T> FixedSizeListBuilder<A, T> {
    pub fn to(self, data_type: DataType) -> FixedSizeListArray {
        let values = self
            .values
            .to_arc(FixedSizeListArray::get_child_and_size(&data_type).0);
        FixedSizeListArray::from_data(data_type, values, self.validity.into())
    }
}

impl<B, T, P> TryExtend<Option<P>> for FixedSizeListBuilder<B, T>
where
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

impl<T, B, P> Extend<Option<P>> for FixedSizeListBuilder<B, T>
where
    B: Builder<T>,
    P: IntoIterator<Item = Option<T>>,
{
    fn extend<I: IntoIterator<Item = Option<P>>>(&mut self, iter: I) {
        self.try_extend(iter).unwrap()
    }
}

impl<T, B> NullableBuilder for FixedSizeListBuilder<B, T>
where
    B: Builder<T>,
{
    #[inline]
    fn push_null(&mut self) {
        self.validity.push(false);
    }
}

impl<T, B, P> Builder<P> for FixedSizeListBuilder<B, T>
where
    B: Builder<T>,
    P: IntoIterator<Item = Option<T>>,
{
    #[inline]
    fn try_push(&mut self, value: P) -> Result<()> {
        value.into_iter().try_for_each(|item| match item {
            Some(item) => self.values.try_push(item),
            None => {
                self.values.push_null();
                Ok(())
            }
        })?;
        self.validity.push(true);
        Ok(())
    }

    #[inline]
    fn push(&mut self, value: P) {
        self.try_push(value).unwrap()
    }
}

impl<B: Builder<T> + ToArray, T> ToArray for FixedSizeListBuilder<B, T> {
    fn to_arc(self, data_type: &DataType) -> Arc<dyn Array> {
        Arc::new(self.to(data_type.clone()))
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
            Some(vec![None, None, None]),
            Some(vec![Some(4), None, Some(6)]),
        ];

        let mut a = FixedSizeListBuilder::<_, i32>::new(PrimitiveBuilder::<i32>::new());
        a.try_extend(data).unwrap();

        let list = a.to(FixedSizeListArray::default_datatype(DataType::Int32, 3));

        let a = list.value(0);
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![Some(1i32), Some(2), Some(3)]);
        assert_eq!(a, &expected);

        let a = list.value(1);
        let a = a.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();

        let expected = Int32Array::from(vec![None, None, None]);
        assert_eq!(a, &expected)
    }
}
