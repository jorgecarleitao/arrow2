use std::sync::Arc;

use crate::{
    array::{Array, Builder, ToArray, TryExtend},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::Result,
};

use super::FixedSizeListArray;

#[derive(Debug)]
pub struct FixedSizeListPrimitive<B: Builder<T>, T> {
    values: B,
    validity: MutableBitmap,
    phantom: std::marker::PhantomData<T>,
}

impl<B: Builder<T>, T> FixedSizeListPrimitive<B, T> {
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

impl<A: Builder<T> + ToArray, T> FixedSizeListPrimitive<A, T> {
    pub fn to(self, data_type: DataType) -> FixedSizeListArray {
        let values = self
            .values
            .to_arc(FixedSizeListArray::get_child_and_size(&data_type).0);
        FixedSizeListArray::from_data(data_type, values, self.validity.into())
    }
}

impl<B, T, P> TryExtend<Option<P>> for FixedSizeListPrimitive<B, T>
where
    B: Builder<T>,
    P: IntoIterator<Item = Option<T>>,
{
    fn try_extend<I: IntoIterator<Item = Option<P>>>(&mut self, iter: I) -> Result<()> {
        for item in iter {
            self.try_push(item)?;
        }
        Ok(())
    }
}

impl<T, B, P> Extend<Option<P>> for FixedSizeListPrimitive<B, T>
where
    B: Builder<T>,
    P: IntoIterator<Item = Option<T>>,
{
    fn extend<I: IntoIterator<Item = Option<P>>>(&mut self, iter: I) {
        self.try_extend(iter).unwrap()
    }
}

impl<T, B, P> Builder<P> for FixedSizeListPrimitive<B, T>
where
    B: Builder<T>,
    P: IntoIterator<Item = Option<T>>,
{
    #[inline]
    fn try_push(&mut self, value: Option<P>) -> Result<()> {
        match value {
            Some(v) => {
                v.into_iter()
                    .try_for_each(|item| self.values.try_push(item))?;
                self.validity.push(true);
            }
            None => {
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

impl<B: Builder<T> + ToArray, T> ToArray for FixedSizeListPrimitive<B, T> {
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

        let mut a = FixedSizeListPrimitive::<_, i32>::new(Primitive::<i32>::new());
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
