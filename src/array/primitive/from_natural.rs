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
    /// Creates a new array out an iterator over values
    pub fn from_values<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::from_data(
            T::DATA_TYPE,
            MutableBuffer::<T>::from_iter(iter).into(),
            None,
        )
    }

    /// Creates a new array out an iterator over values
    pub fn from_slice<P: AsRef<[T]>>(slice: P) -> Self {
        Self::from_data(T::DATA_TYPE, Buffer::<T>::from(slice), None)
    }
}

impl<T: NativeType + NaturalDataType> PrimitiveArray<T> {
    /// Creates a new array out an iterator over values
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

    /// Creates a new [`PrimitiveArray`] from an iterator over optional values
    pub fn from_trusted_len_iter<I: TrustedLen<Item = Option<T>>>(iter: I) -> Self {
        MutablePrimitiveArray::<T>::from_trusted_len_iter(iter).into()
    }

    /// Creates a new [`PrimitiveArray`] from an iterator over optional values
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    pub fn from_trusted_len_iter_unchecked<I: TrustedLen<Item = Option<T>>>(iter: I) -> Self {
        MutablePrimitiveArray::<T>::from_trusted_len_iter(iter).into()
    }
}

#[cfg(test)]
mod tests {
    use crate::array::Array;

    use super::*;
    use std::iter::FromIterator;

    #[test]
    fn bla() {
        let data = vec![Some(1), None, Some(10)];

        let array = PrimitiveArray::from(data.clone());
        assert_eq!(array.len(), 3);

        let array = PrimitiveArray::from_iter(data);
        assert_eq!(array.len(), 3);

        let data = vec![1i32, 2, 3];

        let array = PrimitiveArray::from_values(data.clone());
        assert_eq!(array.len(), 3);

        let array = PrimitiveArray::from_trusted_len_values_iter(data.into_iter());
        assert_eq!(array.len(), 3);
    }
}
