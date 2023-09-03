use std::borrow::Borrow;
use std::fmt::{self, Debug};
use std::hash::{BuildHasher, Hash, Hasher};

use hashbrown::hash_map::RawEntryMut;
use hashbrown::HashMap;

use crate::array::Array;
use crate::{
    array::indexable::{AsIndexed, Indexable},
    array::MutableArray,
    datatypes::DataType,
    error::{Error, Result},
};

use super::DictionaryKey;

pub struct ValueMap<K: DictionaryKey, M: MutableArray> {
    values: M,
    map: HashMap<usize, K>, // NB: *only* use insert_hashed_nocheck() and no other hashmap API
}

impl<K: DictionaryKey, M: MutableArray> ValueMap<K, M> {
    pub fn try_empty(values: M) -> Result<Self> {
        if !values.is_empty() {
            return Err(Error::InvalidArgumentError(
                "initializing value map with non-empty values array".into(),
            ));
        }
        Ok(Self {
            values,
            map: HashMap::default(),
        })
    }

    pub fn from_values(values: M) -> Result<Self>
    where
        M: Indexable,
        M::Type: Eq + Hash,
    {
        let mut map = HashMap::with_capacity(values.len());
        for index in 0..values.len() {
            let key = K::try_from(index).map_err(|_| Error::Overflow)?;
            // safety: we only iterate within bounds
            let value = unsafe { values.value_unchecked_at(index) };
            let mut hasher = map.hasher().build_hasher();
            value.borrow().hash(&mut hasher);
            let hash = hasher.finish();
            match map.raw_entry_mut().from_hash(hash, |_| true) {
                RawEntryMut::Occupied(_) => {
                    return Err(Error::InvalidArgumentError(
                        "duplicate value in dictionary values array".into(),
                    ))
                }
                RawEntryMut::Vacant(entry) => {
                    entry.insert_hashed_nocheck(hash, index, key); // NB: don't use .insert() here!
                }
            }
        }
        Ok(Self { values, map })
    }

    pub fn data_type(&self) -> &DataType {
        self.values.data_type()
    }

    pub fn into_values(self) -> M {
        self.values
    }

    pub fn take_into(&mut self) -> Box<dyn Array> {
        let arr = self.values.as_box();
        self.map.clear();
        arr
    }

    #[inline]
    pub fn values(&self) -> &M {
        &self.values
    }

    /// Try to insert a value and return its index (it may or may not get inserted).
    pub fn try_push_valid<V>(
        &mut self,
        value: V,
        mut push: impl FnMut(&mut M, V) -> Result<()>,
    ) -> Result<K>
    where
        M: Indexable,
        V: AsIndexed<M>,
        M::Type: Eq + Hash,
    {
        let mut hasher = self.map.hasher().build_hasher();
        value.as_indexed().hash(&mut hasher);
        let hash = hasher.finish();

        Ok(
            match self.map.raw_entry_mut().from_hash(hash, |index| {
                // safety: invariant of the struct, it's always in bounds since we maintain it
                let stored_value = unsafe { self.values.value_unchecked_at(*index) };
                stored_value.borrow() == value.as_indexed()
            }) {
                RawEntryMut::Occupied(entry) => *entry.get(),
                RawEntryMut::Vacant(entry) => {
                    let index = self.values.len();
                    let key = K::try_from(index).map_err(|_| Error::Overflow)?;
                    entry.insert_hashed_nocheck(hash, index, key); // NB: don't use .insert() here!
                    push(&mut self.values, value)?;
                    key
                }
            },
        )
    }

    pub fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit();
    }
}

impl<K: DictionaryKey, M: MutableArray> Debug for ValueMap<K, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.values.fmt(f)
    }
}
