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

use std::sync::Arc;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use hash_hasher::HashedMap;

use crate::{
    array::{Array, Builder, Primitive, ToArray, TryFromIterator},
    datatypes::DataType,
    error::{ArrowError, Result},
};

use super::{DictionaryArray, DictionaryKey};

#[derive(Debug)]
pub struct DictionaryPrimitive<K: DictionaryKey, B: Builder<T>, T: Hash> {
    keys: Primitive<K>,
    map: HashedMap<u64, K>,
    values: B,
    phantom: std::marker::PhantomData<T>,
}

impl<K: DictionaryKey, B: Builder<T>, T: Hash> DictionaryPrimitive<K, B, T> {
    pub fn to(self, data_type: DataType) -> DictionaryArray<K> {
        let data_type = DictionaryArray::<K>::get_child(&data_type);
        let values = self.values.to_arc(data_type);
        DictionaryArray::from_data(self.keys.to(K::DATA_TYPE), values)
    }
}

impl<K, B, T> ToArray for DictionaryPrimitive<K, B, T>
where
    K: DictionaryKey,
    B: Builder<T>,
    T: Hash,
{
    fn to_arc(self, data_type: &DataType) -> Arc<dyn Array> {
        Arc::new(self.to(data_type.clone()))
    }
}

impl<K, B, T> TryFromIterator<Option<T>> for DictionaryPrimitive<K, B, T>
where
    K: DictionaryKey,
    B: Builder<T>,
    T: Hash,
{
    fn try_from_iter<I: IntoIterator<Item = Result<Option<T>>>>(iter: I) -> Result<Self> {
        let iterator = iter.into_iter();
        let (lower, _) = iterator.size_hint();
        let mut primitive: DictionaryPrimitive<K, B, T> = Builder::<T>::with_capacity(lower);
        for item in iterator {
            primitive.try_push(item?.as_ref())?;
        }
        Ok(primitive)
    }
}

impl<K, T, B> Builder<T> for DictionaryPrimitive<K, B, T>
where
    K: DictionaryKey,
    B: Builder<T>,
    T: Hash,
{
    #[inline]
    fn with_capacity(capacity: usize) -> Self {
        Self {
            keys: Primitive::<K>::with_capacity(capacity),
            values: B::with_capacity(0),
            map: HashedMap::<u64, K>::default(),
            phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    fn try_push(&mut self, value: Option<&T>) -> Result<()> {
        match value {
            Some(v) => {
                let mut hasher = DefaultHasher::new();
                v.hash(&mut hasher);
                let hash = hasher.finish();
                match self.map.get(&hash) {
                    Some(key) => self.keys.push(Some(key)),
                    None => {
                        let key = K::from_usize(self.map.len())
                            .ok_or(ArrowError::DictionaryKeyOverflowError)?;
                        self.values.try_push(value)?;
                        self.map.insert(hash, key);
                        self.keys.push(Some(&key));
                    }
                }
            }
            None => {
                self.keys.push(None);
            }
        }
        Ok(())
    }

    #[inline]
    fn push(&mut self, value: Option<&T>) {
        self.try_push(value).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::array::Utf8Primitive;

    use super::*;

    #[test]
    fn primitive() -> Result<()> {
        let data = vec![Some("a"), Some("b"), Some("a")];

        let data = data.into_iter().map(|x| Result::Ok(x));
        let a = DictionaryPrimitive::<i32, Utf8Primitive<i32>, &str>::try_from_iter(data)?;
        a.to(DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        ));
        Ok(())
    }
}
