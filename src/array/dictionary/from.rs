use std::sync::Arc;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use hash_hasher::HashedMap;

use crate::array::TryExtend;
use crate::{
    array::{Array, Builder, IntoArray, Primitive, ToArray},
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
    pub fn new(values: B) -> Self {
        Self::with_capacity(0, values)
    }

    pub fn with_capacity(capacity: usize, values: B) -> Self {
        Self {
            keys: Primitive::<K>::with_capacity(capacity),
            values,
            map: HashedMap::<u64, K>::default(),
            phantom: std::marker::PhantomData,
        }
    }
}

impl<K: DictionaryKey, B: Builder<T> + ToArray, T: Hash> DictionaryPrimitive<K, B, T> {
    pub fn to(self, data_type: DataType) -> DictionaryArray<K> {
        let data_type = DictionaryArray::<K>::get_child(&data_type);
        let values = self.values.to_arc(data_type);
        DictionaryArray::from_data(self.keys.to(K::DATA_TYPE), values)
    }
}

impl<K: DictionaryKey, B: Builder<T> + IntoArray, T: Hash> DictionaryPrimitive<K, B, T> {
    pub fn into(self) -> DictionaryArray<K> {
        DictionaryArray::from_data(self.keys.to(K::DATA_TYPE), self.values.into_arc())
    }
}

impl<K, B, T> ToArray for DictionaryPrimitive<K, B, T>
where
    K: DictionaryKey,
    B: Builder<T> + ToArray,
    T: Hash,
{
    fn to_arc(self, data_type: &DataType) -> Arc<dyn Array> {
        Arc::new(self.to(data_type.clone()))
    }
}

impl<K, B, T> IntoArray for DictionaryPrimitive<K, B, T>
where
    K: DictionaryKey,
    B: Builder<T> + IntoArray,
    T: Hash,
{
    fn into_arc(self) -> Arc<dyn Array> {
        Arc::new(self.into())
    }
}

impl<K, B, T> TryExtend<Option<T>> for DictionaryPrimitive<K, B, T>
where
    K: DictionaryKey,
    B: Builder<T>,
    T: Hash,
{
    fn try_extend<I: IntoIterator<Item = Option<T>>>(&mut self, iter: I) -> Result<()> {
        for item in iter {
            self.try_push(item)?
        }
        Ok(())
    }
}

impl<K, B, T> Extend<Option<T>> for DictionaryPrimitive<K, B, T>
where
    K: DictionaryKey,
    B: Builder<T>,
    T: Hash,
{
    fn extend<I: IntoIterator<Item = Option<T>>>(&mut self, iter: I) {
        self.try_extend(iter).unwrap()
    }
}

impl<K, T, B> Builder<T> for DictionaryPrimitive<K, B, T>
where
    K: DictionaryKey,
    B: Builder<T>,
    T: Hash,
{
    #[inline]
    fn try_push(&mut self, value: Option<T>) -> Result<()> {
        match value {
            Some(v) => {
                let mut hasher = DefaultHasher::new();
                v.hash(&mut hasher);
                let hash = hasher.finish();
                match self.map.get(&hash) {
                    Some(key) => self.keys.push(Some(*key)),
                    None => {
                        let key = K::from_usize(self.map.len())
                            .ok_or(ArrowError::DictionaryKeyOverflowError)?;
                        self.values.try_push(Some(v))?;
                        self.map.insert(hash, key);
                        self.keys.push(Some(key));
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
    fn push(&mut self, value: Option<T>) {
        self.try_push(value).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{BinaryPrimitive, Utf8Primitive};

    use super::*;

    #[test]
    fn primitive() -> Result<()> {
        let data = vec![Some("a"), Some("b"), Some("a")];

        let mut a = DictionaryPrimitive::<i32, _, _>::new(Utf8Primitive::<i32>::new());

        a.try_extend(data)?;
        let a = a.into();
        assert_eq!(a.len(), 3);
        assert_eq!(a.values().len(), 2);
        Ok(())
    }

    #[test]
    fn utf8_natural() -> Result<()> {
        let data = vec![Some("a"), Some("b"), Some("a")];

        let mut a = DictionaryPrimitive::<i32, _, _>::new(Utf8Primitive::<i32>::new());
        a.try_extend(data)?;

        let a = a.into_arc();
        assert_eq!(a.len(), 3);
        let a = a.as_any().downcast_ref::<DictionaryArray<i32>>().unwrap();
        assert_eq!(a.values().len(), 2);
        Ok(())
    }

    #[test]
    fn binary_natural() -> Result<()> {
        let data = vec![Some("a".as_ref()), Some("b".as_ref()), Some("a".as_ref())];

        let mut a = DictionaryPrimitive::<i32, _, _>::new(BinaryPrimitive::<i32>::new());
        a.try_extend(data)?;
        let a = a.into_arc();
        let a = a.as_any().downcast_ref::<DictionaryArray<i32>>().unwrap();
        assert_eq!(a.len(), 3);
        assert_eq!(a.values().len(), 2);
        Ok(())
    }
}
