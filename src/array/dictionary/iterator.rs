use crate::array::{ArrayAccessor, ArrayValuesIter};
use crate::bitmap::utils::{BitmapIter, ZipValidity};
use crate::scalar::Scalar;

use super::{DictionaryArray, DictionaryKey};

unsafe impl<'a, K> ArrayAccessor<'a> for DictionaryArray<K>
where
    K: DictionaryKey,
{
    type Item = Box<dyn Scalar>;

    #[inline]
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        // safety: invariant of the trait
        self.value_unchecked(index)
    }

    #[inline]
    fn len(&self) -> usize {
        self.keys.len()
    }
}

/// Iterator of values of a [`DictionaryArray`].
pub type DictionaryValuesIter<'a, K> = ArrayValuesIter<'a, DictionaryArray<K>>;

type ValuesIter<'a, K> = DictionaryValuesIter<'a, K>;
type ZipIter<'a, K> = ZipValidity<Box<dyn Scalar>, ValuesIter<'a, K>, BitmapIter<'a>>;

impl<'a, K: DictionaryKey> IntoIterator for &'a DictionaryArray<K> {
    type Item = Option<Box<dyn Scalar>>;
    type IntoIter = ZipIter<'a, K>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
