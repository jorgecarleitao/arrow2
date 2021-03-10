// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{iter::FromIterator, sync::Arc};

use crate::{
    array::{Array, Builder, Offset, ToArray, TryFromIterator},
    buffer::{MutableBitmap, MutableBuffer},
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

impl<O: Offset, A: Builder<T>, T> ListPrimitive<O, A, T> {
    pub fn to(self, data_type: DataType) -> ListArray<O> {
        let values = self.values.to_arc(ListArray::<O>::get_child(&data_type));
        ListArray::from_data(data_type, self.offsets.into(), values, self.validity.into())
    }
}

impl<O, B, T, P> FromIterator<Option<P>> for ListPrimitive<O, B, T>
where
    O: Offset,
    B: Builder<T>,
    P: AsRef<[Option<T>]> + IntoIterator<Item = Option<T>>,
{
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        Self::try_from_iter(iter.into_iter().map(Ok)).unwrap()
    }
}

impl<O, B, T, P> TryFromIterator<Option<P>> for ListPrimitive<O, B, T>
where
    O: Offset,
    B: Builder<T>,
    P: AsRef<[Option<T>]> + IntoIterator<Item = Option<T>>,
{
    fn try_from_iter<I: IntoIterator<Item = Result<Option<P>>>>(iter: I) -> Result<Self> {
        let iterator = iter.into_iter();
        let (lower, _) = iterator.size_hint();
        let mut primitive: ListPrimitive<O, B, T> = Builder::<P>::with_capacity(lower);
        for item in iterator {
            primitive.try_push(item?.as_ref())?;
        }
        Ok(primitive)
    }
}

impl<O, T, B, P> Builder<P> for ListPrimitive<O, B, T>
where
    O: Offset,
    B: Builder<T>,
    P: AsRef<[Option<T>]> + IntoIterator<Item = Option<T>>,
{
    #[inline]
    fn with_capacity(capacity: usize) -> Self {
        let mut offsets = MutableBuffer::<O>::with_capacity(capacity + 1);
        let length = O::default();
        unsafe { offsets.push_unchecked(length) };

        Self {
            offsets,
            values: B::with_capacity(0),
            validity: MutableBitmap::with_capacity(capacity),
            length,
            phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    fn try_push(&mut self, value: Option<&P>) -> Result<()> {
        match value {
            Some(v) => {
                let items = v.as_ref();
                let length =
                    O::from_usize(items.len()).ok_or(ArrowError::DictionaryKeyOverflowError)?;
                self.length += length;
                self.offsets.push(self.length);
                items
                    .iter()
                    .try_for_each(|item| self.values.try_push(item.as_ref()))?;
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
    fn push(&mut self, value: Option<&P>) {
        self.try_push(value).unwrap()
    }
}

impl<O: Offset, B: Builder<T>, T> ToArray for ListPrimitive<O, B, T> {
    fn to_arc(self, data_type: &DataType) -> Arc<dyn Array> {
        Arc::new(self.to(data_type.clone()))
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{Primitive, PrimitiveArray};

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
}
