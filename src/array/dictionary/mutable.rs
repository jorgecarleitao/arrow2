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
    /// Creates an empty [`MutableDictionaryArray`].
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

    /// pushes a null value
    pub fn push_null(&mut self) {
        self.keys.push(None)
    }

    /// returns a mutable reference to the inner values.
    pub fn mut_values(&mut self) -> &mut M {
        &mut self.values
    }

    /// returns a reference to the inner values.
    pub fn values(&self) -> &M {
        &self.values
    }

    /// converts itself into `Arc<dyn Array>`
    pub fn into_arc(self) -> Arc<dyn Array> {
        let a: DictionaryArray<K> = self.into();
        Arc::new(a)
    }

    /// Shrinks the capacity of the [`MutableDictionaryArray`] to fit its current length.
    pub fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit();
        self.keys.shrink_to_fit();
    }
}

impl<K: DictionaryKey, M: 'static + MutableArray> MutableArray for MutableDictionaryArray<K, M> {
    fn len(&self) -> usize {
        self.keys.len()
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        self.keys.validity()
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(DictionaryArray::<K>::from_data(
            std::mem::take(&mut self.keys).into(),
            self.values.as_arc(),
        ))
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
    fn shrink_to_fit(&mut self) {
        self.shrink_to_fit()
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
