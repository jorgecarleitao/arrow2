use std::hash::{Hash, Hasher};
use std::{collections::hash_map::DefaultHasher, sync::Arc};

use hash_hasher::HashedMap;

use crate::array::TryExtend;
use crate::{
    array::{primitive::MutablePrimitiveArray, Array, MutableArray},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::{ArrowError, Result},
};

use super::{DictionaryArray, DictionaryKey};

/// A mutable, strong-typed version of [`DictionaryArray`].
#[derive(Debug)]
pub struct MutableDictionaryArray<K: DictionaryKey, M: MutableArray> {
    data_type: DataType,
    keys: MutablePrimitiveArray<K>,
    map: HashedMap<u64, K>,
    values: M,
}

impl<K: DictionaryKey, M: MutableArray> From<MutableDictionaryArray<K, M>> for DictionaryArray<K> {
    fn from(mut other: MutableDictionaryArray<K, M>) -> Self {
        DictionaryArray::<K>::from_data(other.keys.into(), other.values.as_arc())
    }
}

impl<K: DictionaryKey, M: MutableArray> From<M> for MutableDictionaryArray<K, M> {
    fn from(values: M) -> Self {
        Self {
            data_type: DataType::Dictionary(
                Box::new(K::DATA_TYPE),
                Box::new(values.data_type().clone()),
            ),
            keys: MutablePrimitiveArray::<K>::new(),
            map: HashedMap::default(),
            values,
        }
    }
}

impl<K: DictionaryKey, M: MutableArray + Default> MutableDictionaryArray<K, M> {
    pub fn new() -> Self {
        let values = M::default();
        Self {
            data_type: DataType::Dictionary(
                Box::new(K::DATA_TYPE),
                Box::new(values.data_type().clone()),
            ),
            keys: MutablePrimitiveArray::<K>::new(),
            map: HashedMap::default(),
            values,
        }
    }
}

impl<K: DictionaryKey, M: MutableArray + Default> Default for MutableDictionaryArray<K, M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: DictionaryKey, M: MutableArray> MutableDictionaryArray<K, M> {
    /// Returns whether the value should be pushed to the values or not
    pub fn try_push_valid<T: Hash>(&mut self, value: &T) -> Result<bool> {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        let hash = hasher.finish();
        match self.map.get(&hash) {
            Some(key) => {
                self.keys.push(Some(*key));
                Ok(false)
            }
            None => {
                let key = K::from_usize(self.map.len()).ok_or(ArrowError::KeyOverflowError)?;
                self.map.insert(hash, key);
                self.keys.push(Some(key));
                Ok(true)
            }
        }
    }

    pub fn push_null(&mut self) {
        self.keys.push(None)
    }

    pub fn mut_values(&mut self) -> &mut M {
        &mut self.values
    }

    pub fn values(&self) -> &M {
        &self.values
    }

    pub fn into_arc(self) -> Arc<dyn Array> {
        let a: DictionaryArray<K> = self.into();
        Arc::new(a)
    }
}

impl<K: DictionaryKey, M: 'static + MutableArray> MutableArray for MutableDictionaryArray<K, M> {
    fn len(&self) -> usize {
        self.keys.len()
    }

    fn validity(&self) -> &Option<MutableBitmap> {
        self.keys.validity()
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(DictionaryArray::<K>::from_data(
            std::mem::take(&mut self.keys).into(),
            self.values.as_arc(),
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
        self.keys.push(None)
    }
}

impl<K, M, T: Hash> TryExtend<Option<T>> for MutableDictionaryArray<K, M>
where
    K: DictionaryKey,
    M: MutableArray + TryExtend<Option<T>>,
{
    fn try_extend<II: IntoIterator<Item = Option<T>>>(&mut self, iter: II) -> Result<()> {
        for value in iter {
            if let Some(value) = value {
                if self.try_push_valid(&value)? {
                    self.mut_values().try_extend(std::iter::once(Some(value)))?;
                }
            } else {
                self.push_null();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{MutableBinaryArray, MutableUtf8Array};

    #[test]
    fn primitive() -> Result<()> {
        let data = vec![Some(1), Some(2), Some(1)];

        let mut a = MutableDictionaryArray::<i32, MutablePrimitiveArray<i32>>::new();
        a.try_extend(data)?;
        assert_eq!(a.len(), 3);
        assert_eq!(a.values().len(), 2);
        Ok(())
    }

    #[test]
    fn utf8_natural() -> Result<()> {
        let data = vec![Some("a"), Some("b"), Some("a")];

        let mut a = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
        a.try_extend(data)?;

        assert_eq!(a.len(), 3);
        assert_eq!(a.values().len(), 2);
        Ok(())
    }

    #[test]
    fn binary_natural() -> Result<()> {
        let data = vec![
            Some("a".as_bytes()),
            Some("b".as_bytes()),
            Some("a".as_bytes()),
        ];

        let mut a = MutableDictionaryArray::<i32, MutableBinaryArray<i32>>::new();
        a.try_extend(data)?;
        assert_eq!(a.len(), 3);
        assert_eq!(a.values().len(), 2);
        Ok(())
    }
}
