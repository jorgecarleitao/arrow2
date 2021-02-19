use std::sync::Arc;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use hash_hasher::HashedMap;

use crate::{
    array::{Array, Builder, Primitive, ToArray},
    datatypes::DataType,
    error::{ArrowError, Result},
};

use super::{DictionaryArray, DictionaryKey};

#[derive(Debug)]
pub struct DictionaryPrimitive<K: DictionaryKey, A: ToArray> {
    keys: Primitive<K>,
    map: HashedMap<u64, K>,
    values: A,
}

impl<K: DictionaryKey, A: ToArray> DictionaryPrimitive<K, A> {
    pub fn to(self, data_type: DataType) -> DictionaryArray<K> {
        let data_type = DictionaryArray::<K>::get_child(&data_type);
        let values = self.values.to_arc(data_type);
        DictionaryArray::from_data(self.keys.to(K::DATA_TYPE), values)
    }
}

pub fn dict_from_iter<K, B, T, P, I>(iter: I) -> Result<DictionaryPrimitive<K, B>>
where
    K: DictionaryKey,
    B: Builder<T>,
    T: Hash,
    P: std::borrow::Borrow<Option<T>>,
    I: IntoIterator<Item = P>,
{
    let mut map = HashedMap::<u64, K>::default();

    let iterator = iter.into_iter();

    // [10, 20, 10, 30]
    // keys = [0, 1, 0, 2]
    // values = [10, 20, 30]

    // if value not in set {
    //     key.push(set.len());
    //     set.insert(value)
    // } else {
    //     key.push(set.len())
    // }
    let mut values = B::with_capacity(0);
    let keys: Primitive<K> = iterator
        .map(|item| match item.borrow() {
            Some(v) => {
                let mut hasher = DefaultHasher::new();
                v.hash(&mut hasher);
                let hash = hasher.finish();
                match map.get(&hash) {
                    Some(key) => Ok(Some(*key)),
                    None => {
                        let key = K::from_usize(map.len())
                            .ok_or(ArrowError::DictionaryKeyOverflowError)?;
                        values.push(Some(v));
                        map.insert(hash, key);
                        Ok(Some(key))
                    }
                }
            }
            None => Ok(None),
        })
        .collect::<Result<_>>()?;

    Ok(DictionaryPrimitive::<K, B> { keys, values, map })
}

impl<K: DictionaryKey, A: ToArray> ToArray for DictionaryPrimitive<K, A> {
    fn to_arc(self, data_type: &DataType) -> Arc<dyn Array> {
        Arc::new(self.to(data_type.clone()))
    }
}

#[cfg(test)]
mod tests {
    use crate::array::Utf8Primitive;

    use super::*;

    #[test]
    fn primitive() -> Result<()> {
        let data = vec![Some("a"), Some("b"), Some("a")];

        let a: DictionaryPrimitive<i32, Utf8Primitive<i32>> = dict_from_iter(data)?;
        a.to(DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        ));
        Ok(())
    }
}
