use std::hash::{Hash, Hasher};
use std::{collections::hash_map::DefaultHasher, sync::Arc};

use hash_hasher::HashedMap;

use crate::array::ArrayAccessor;
use crate::{
    array::{primitive::MutablePrimitiveArray, Array, MutableArray, TryExtend, TryPush},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::{Error, Result},
};

use super::{DictionaryArray, DictionaryKey};

/// A mutable, strong-typed version of [`DictionaryArray`].
///
/// # Example
/// Building a UTF8 dictionary with `i32` keys.
/// ```
/// # use arrow2::array::{MutableDictionaryArray, MutableUtf8Array, TryPush};
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut array: MutableDictionaryArray<i32, MutableUtf8Array<i32>> = MutableDictionaryArray::new();
/// array.try_push(Some("A"))?;
/// array.try_push(Some("B"))?;
/// array.push_null();
/// array.try_push(Some("C"))?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct MutableDictionaryArray<K: DictionaryKey, M: MutableArray> {
    data_type: DataType,
    keys: MutablePrimitiveArray<K>,
    map: HashedMap<u64, K>,
    // invariant: `keys.len() <= values.len()`
    values: M,
}

impl<K: DictionaryKey, M: MutableArray> From<MutableDictionaryArray<K, M>> for DictionaryArray<K> {
    fn from(mut other: MutableDictionaryArray<K, M>) -> Self {
        // Safety - the invariant of this struct ensures that this is up-held
        unsafe {
            DictionaryArray::<K>::try_new_unchecked(
                other.data_type,
                other.keys.into(),
                other.values.as_box(),
            )
            .unwrap()
        }
    }
}

impl<K: DictionaryKey, M: MutableArray> From<M> for MutableDictionaryArray<K, M> {
    fn from(values: M) -> Self {
        Self {
            data_type: DataType::Dictionary(
                K::KEY_TYPE,
                Box::new(values.data_type().clone()),
                false,
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
                K::KEY_TYPE,
                Box::new(values.data_type().clone()),
                false,
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
    fn try_push_valid<T: Hash>(&mut self, value: &T) -> Result<bool> {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        let hash = hasher.finish();
        match self.map.get(&hash) {
            Some(key) => {
                self.keys.push(Some(*key));
                Ok(false)
            }
            None => {
                let key = K::try_from(self.map.len()).map_err(|_| Error::Overflow)?;
                self.map.insert(hash, key);
                self.keys.push(Some(key));
                Ok(true)
            }
        }
    }

    /// pushes a null value
    #[inline]
    pub fn push_null(&mut self) {
        if self.values.is_empty() {
            // keys's default value is 0. If self.values is empty, the 0th index
            // would be out of bound.
            self.values.push_null()
        }
        self.keys.push(None);
    }

    /// returns a mutable reference to the inner values.
    fn mut_values(&mut self) -> &mut M {
        &mut self.values
    }

    /// returns a reference to the inner values.
    pub fn values(&self) -> &M {
        &self.values
    }

    /// converts itself into [`Arc<dyn Array>`]
    pub fn into_arc(self) -> Arc<dyn Array> {
        let a: DictionaryArray<K> = self.into();
        Arc::new(a)
    }

    /// converts itself into [`Box<dyn Array>`]
    pub fn into_box(self) -> Box<dyn Array> {
        let a: DictionaryArray<K> = self.into();
        Box::new(a)
    }

    /// Reserves `additional` slots.
    pub fn reserve(&mut self, additional: usize) {
        self.keys.reserve(additional);
    }

    /// Shrinks the capacity of the [`MutableDictionaryArray`] to fit its current length.
    pub fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit();
        self.keys.shrink_to_fit();
    }

    /// Returns the dictionary map
    pub fn map(&self) -> &HashedMap<u64, K> {
        &self.map
    }

    /// Returns the dictionary keys
    pub fn keys(&self) -> &MutablePrimitiveArray<K> {
        &self.keys
    }

    fn take_into(&mut self) -> DictionaryArray<K> {
        // Safety - the invariant of this struct ensures that this is up-held
        unsafe {
            DictionaryArray::<K>::try_new(
                self.data_type.clone(),
                std::mem::take(&mut self.keys).into(),
                self.values.as_box(),
            )
            .unwrap()
        }
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
        Box::new(self.take_into())
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(self.take_into())
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

    #[inline]
    fn push_null(&mut self) {
        self.push_null()
    }

    fn reserve(&mut self, additional: usize) {
        self.reserve(additional)
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

impl<K, M, T> TryPush<Option<T>> for MutableDictionaryArray<K, M>
where
    K: DictionaryKey,
    M: MutableArray + TryPush<Option<T>>,
    T: Hash,
{
    fn try_push(&mut self, item: Option<T>) -> Result<()> {
        if let Some(value) = item {
            if self.try_push_valid(&value)? {
                self.values.try_push(Some(value))
            } else {
                Ok(())
            }
        } else {
            self.push_null();
            Ok(())
        }
    }
}

unsafe impl<'a, K, M, T: 'a> ArrayAccessor<'a> for MutableDictionaryArray<K, M>
where
    K: DictionaryKey,
    M: MutableArray + ArrayAccessor<'a, Item = T>,
{
    type Item = T;

    #[inline]
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        // safety: invariant of the trait
        let index = self.keys.value_unchecked(index);
        // safety: invariant of the struct
        let index = index.as_usize();
        // safety: invariant of the struct
        self.values.value_unchecked(index)
    }

    #[inline]
    fn len(&self) -> usize {
        self.keys.len()
    }
}
