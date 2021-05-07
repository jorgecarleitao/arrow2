use std::iter::FromIterator;

use crate::{
    buffer::MutableBuffer,
    trusted_len::TrustedLen,
    types::{NativeType, NaturalDataType},
};

use super::{Primitive, PrimitiveArray};

impl<T: NativeType + NaturalDataType, P: AsRef<[Option<T>]>> From<P> for PrimitiveArray<T> {
    fn from(slice: P) -> Self {
        Primitive::<T>::from_trusted_len_iter(slice.as_ref().iter().map(|x| x.as_ref()))
            .to(T::DATA_TYPE)
    }
}

impl<T: NativeType + NaturalDataType, Ptr: std::borrow::Borrow<Option<T>>> FromIterator<Ptr>
    for PrimitiveArray<T>
{
    fn from_iter<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
        Primitive::<T>::from_iter(iter).to(T::DATA_TYPE)
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
}

impl<T: NativeType + NaturalDataType> PrimitiveArray<T> {
    /// Creates a new array out an iterator over values
    pub fn from_trusted_len_values_iter<I: TrustedLen<Item = T>>(iter: I) -> Self {
        Self::from_data(
            T::DATA_TYPE,
            MutableBuffer::<T>::from_trusted_len_iter(iter).into(),
            None,
        )
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
