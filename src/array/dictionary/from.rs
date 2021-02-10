use std::collections::HashMap;
use std::hash::Hash;

use crate::{
    array::{Builder, Primitive, ToArray},
    datatypes::DataType,
    error::{ArrowError, Result},
};

use super::{DictionaryArray, DictionaryKey};

#[derive(Debug)]
pub struct DictionaryPrimitive<K: DictionaryKey, A: ToArray> {
    keys: Primitive<K>,
    values: A,
}

impl<K: DictionaryKey, A: ToArray> DictionaryPrimitive<K, A> {
    pub fn to(self, data_type: DataType) -> DictionaryArray<K> {
        let (keys, values) = DictionaryArray::<K>::get_child(&data_type);
        let values = self.values.to_arc(values);
        DictionaryArray::from_data(self.keys.to(keys.clone()), values)
    }
}

pub fn dict_from_iter<K, B, T, P, I>(iter: I) -> Result<DictionaryPrimitive<K, B>>
where
    K: DictionaryKey,
    B: Builder<T>,
    T: Eq + Hash + Clone,
    P: std::borrow::Borrow<Option<T>>,
    I: IntoIterator<Item = P>,
{
    let mut map = HashMap::<T, K>::new();

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
            Some(v) => match map.get(v) {
                Some(key) => Ok(Some(*key)),
                None => {
                    let key =
                        K::from_usize(map.len()).ok_or(ArrowError::DictionaryKeyOverflowError)?;
                    values.push(Some(v));
                    map.insert(v.clone(), key);
                    Ok(Some(key))
                }
            },
            None => Ok(None),
        })
        .collect::<Result<_>>()?;

    Ok(DictionaryPrimitive::<K, B> { keys, values })
}
