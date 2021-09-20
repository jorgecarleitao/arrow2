use std::iter::FromIterator;

use crate::{
    buffer::{Buffer, MutableBuffer},
    trusted_len::TrustedLen,
    types::{NativeType, NaturalDataType},
};

use super::{MutablePrimitiveArray, PrimitiveArray};

impl<T: NativeType + NaturalDataType, P: AsRef<[Option<T>]>> From<P> for PrimitiveArray<T> {
    fn from(slice: P) -> Self {
        MutablePrimitiveArray::<T>::from(slice).into()
    }
}

impl<T: NativeType + NaturalDataType, Ptr: std::borrow::Borrow<Option<T>>> FromIterator<Ptr>
    for PrimitiveArray<T>
{
    fn from_iter<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
        MutablePrimitiveArray::<T>::from_iter(iter).into()
    }
}

impl<T: NativeType + NaturalDataType> PrimitiveArray<T> {
    /// Creates a (non-null) [`PrimitiveArray`] from an iterator of values.
    /// # Implementation
    /// This does not assume that the iterator has a known length.
    pub fn from_values<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::from_data(
            T::DATA_TYPE,
            MutableBuffer::<T>::from_iter(iter).into(),
            None,
        )
    }

    /// Creates a (non-null) [`PrimitiveArray`] from a slice of values.
    /// # Implementation
    /// This is essentially a memcopy and is the fastest way to create a [`PrimitiveArray`].
    pub fn from_slice<P: AsRef<[T]>>(slice: P) -> Self {
        Self::from_data(T::DATA_TYPE, Buffer::<T>::from(slice), None)
    }
}

impl<T: NativeType + NaturalDataType> PrimitiveArray<T> {
    /// Creates a (non-null) [`PrimitiveArray`] from a [`TrustedLen`] of values.
    /// # Implementation
    /// This does not assume that the iterator has a known length.
    pub fn from_trusted_len_values_iter<I: TrustedLen<Item = T>>(iter: I) -> Self {
        MutablePrimitiveArray::<T>::from_trusted_len_values_iter(iter).into()
    }

    /// Creates a new [`PrimitiveArray`] from an iterator over values
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    pub unsafe fn from_trusted_len_values_iter_unchecked<I: Iterator<Item = T>>(iter: I) -> Self {
        MutablePrimitiveArray::<T>::from_trusted_len_values_iter_unchecked(iter).into()
    }

    /// Creates a [`PrimitiveArray`] from a [`TrustedLen`] of optional values.
    pub fn from_trusted_len_iter<I: TrustedLen<Item = Option<T>>>(iter: I) -> Self {
        MutablePrimitiveArray::<T>::from_trusted_len_iter(iter).into()
    }

    /// Creates a [`PrimitiveArray`] from an iterator of optional values.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    pub unsafe fn from_trusted_len_iter_unchecked<I: Iterator<Item = Option<T>>>(iter: I) -> Self {
        MutablePrimitiveArray::<T>::from_trusted_len_iter_unchecked(iter).into()
    }
}
