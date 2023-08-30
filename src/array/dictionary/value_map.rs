use std::borrow::Borrow;
use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::ptr::NonNull;

use hashbrown::{Equivalent, HashMap};

use crate::array::Array;
use crate::{
    array::indexable::{AsIndexed, Indexable},
    array::MutableArray,
    datatypes::DataType,
    error::{Error, Result},
};

use super::DictionaryKey;

struct NonNullSend<M: ?Sized>(NonNull<M>);

// safety: these pointers are for internal self-referential purposes to pinned array only
unsafe impl<M> Send for NonNullSend<M> {}
unsafe impl<M> Sync for NonNullSend<M> {}

impl<M: ?Sized> From<&M> for NonNullSend<M> {
    #[inline]
    fn from(reference: &M) -> Self {
        Self(NonNull::from(reference))
    }
}

struct ValueRef<M> {
    array: NonNullSend<M>,
    index: usize,
}

impl<M> ValueRef<M> {
    #[inline]
    pub fn new(array: &Pin<Box<M>>, index: usize) -> Self {
        Self {
            array: NonNullSend::from(Pin::get_ref(array.as_ref())),
            index,
        }
    }

    #[inline]
    pub fn get_array(&self) -> &M {
        // safety: the invariant of the struct
        unsafe { self.array.0.as_ref() }
    }

    #[inline]
    pub unsafe fn get_unchecked(&self) -> M::Value<'_>
    where
        M: Indexable,
    {
        self.get_array().value_unchecked_at(self.index)
    }

    #[inline]
    pub unsafe fn equals(&self, other: &M::Type) -> bool
    where
        M: Indexable,
        M::Type: Eq,
    {
        self.get_unchecked().borrow() == other
    }
}

impl<M: Indexable> PartialEq for ValueRef<M>
where
    M::Type: PartialEq,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        // safety: the way these value refs are constructed, they are always within bounds
        unsafe {
            self.get_unchecked()
                .borrow()
                .eq(other.get_unchecked().borrow())
        }
    }
}

impl<M: Indexable> Eq for ValueRef<M> where for<'a> M::Type: Eq {}

impl<M: Indexable> Hash for ValueRef<M>
where
    M::Type: Hash,
{
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        // safety: the way these value refs are constructed, they are always within bounds
        unsafe { self.get_unchecked().borrow().hash(state) }
    }
}

// To avoid blanket implementation issues with `Equivalent` trait (we only use hashbrown
// instead of the default HashMap to avoid blanket implementation problems with Borrow).
#[derive(Hash)]
struct Wrapped<'a, T: ?Sized>(&'a T);

impl<'a, M: Indexable> Equivalent<ValueRef<M>> for Wrapped<'a, M::Type>
where
    M::Type: Eq,
{
    #[inline]
    fn equivalent(&self, key: &ValueRef<M>) -> bool {
        // safety: invariant of the struct
        unsafe { key.equals(self.0) }
    }
}

pub struct ValueMap<K: DictionaryKey, M: MutableArray> {
    values: Pin<Box<M>>,
    map: HashMap<ValueRef<M>, K>,
}

impl<K: DictionaryKey, M: MutableArray> ValueMap<K, M> {
    pub fn try_empty(values: M) -> Result<Self> {
        if !values.is_empty() {
            return Err(Error::InvalidArgumentError(
                "initializing value map with non-empty values array".into(),
            ));
        }
        Ok(Self {
            values: Box::pin(values),
            map: HashMap::default(),
        })
    }

    pub fn from_values(values: M) -> Result<Self>
    where
        M: Indexable,
        M::Type: Eq + Hash,
    {
        let values = Box::pin(values);
        let map = (0..values.len())
            .map(|i| {
                let key = K::try_from(i).map_err(|_| Error::Overflow)?;
                Ok((ValueRef::new(&values, i), key))
            })
            .collect::<Result<_>>()?;
        Ok(Self { values, map })
    }

    pub fn data_type(&self) -> &DataType {
        Pin::get_ref(self.values.as_ref()).data_type()
    }

    pub fn into_boxed(self) -> Box<M> {
        // safety: we unpin the pointer but the value map is dropped along with all
        // the value references that might refer to the pinned array
        unsafe { Pin::into_inner_unchecked(self.values) }
    }

    pub fn take_into(&mut self) -> Box<dyn Array> {
        // safety: we unpin the pointer but the value map is manually cleared
        let arr = unsafe { self.values.as_mut().get_unchecked_mut().as_box() };
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
        if let Some(&key) = self.map.get(&Wrapped(value.as_indexed())) {
            return Ok(key);
        }
        let index = self.values.len();
        let key = K::try_from(index).map_err(|_| Error::Overflow)?;
        // safety: we don't move the data out of the mutable pinned reference
        unsafe {
            push(self.values.as_mut().get_unchecked_mut(), value)?;
        }
        debug_assert_eq!(self.values.len(), index + 1);
        self.map.insert(ValueRef::new(&self.values, index), key);
        debug_assert_eq!(self.values.len(), self.map.len());
        Ok(key)
    }

    pub fn shrink_to_fit(&mut self) {
        // safety: we don't move the data out of the mutable pinned reference
        unsafe {
            self.values.as_mut().get_unchecked_mut().shrink_to_fit();
        }
    }
}

impl<K: DictionaryKey, M: MutableArray> Debug for ValueMap<K, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Pin::get_ref(self.values.as_ref()).fmt(f)
    }
}
