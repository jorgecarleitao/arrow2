use std::{iter::FromIterator, sync::Arc};

use crate::{
    array::{
        Array, Builder, IntoArray, NullableBuilder, Offset, ToArray, TryExtend, TryFromIterator,
    },
    bitmap::{Bitmap, MutableBitmap},
    buffer::{Buffer, MutableBuffer},
    datatypes::DataType,
    error::{ArrowError, Result as ArrowResult},
    trusted_len::TrustedLen,
};

use super::BinaryArray;

impl<O: Offset> BinaryArray<O> {
    pub fn from_slice<T: AsRef<[u8]>, P: AsRef<[T]>>(slice: P) -> Self {
        Self::from_iter(slice.as_ref().iter().map(Some))
    }

    /// Creates a new [`BinaryArray`] from a slice of `&[u8]`.
    // Note: this can't be `impl From` because Rust does not allow double `AsRef` on it.
    pub fn from<T: AsRef<[u8]>, P: AsRef<[Option<T>]>>(slice: P) -> Self {
        Self::from_trusted_len_iter(slice.as_ref().iter().map(|x| x.as_ref()))
    }

    /// Creates a [`BinaryArray`] from an iterator of trusted length.
    #[inline]
    pub fn from_trusted_len_iter<I, P>(iterator: I) -> Self
    where
        P: AsRef<[u8]>,
        I: TrustedLen<Item = Option<P>>,
    {
        // soundness: I is `TrustedLen`
        let (validity, offsets, values) = unsafe { trusted_len_unzip(iterator) };

        Self::from_data(offsets, values, validity)
    }
}

/// auxiliary struct used to create a [`BinaryArray`] out of an iterator
#[derive(Debug)]
pub struct BinaryPrimitive<O: Offset> {
    offsets: MutableBuffer<O>,
    values: MutableBuffer<u8>,
    validity: MutableBitmap,
    // invariant: always equal to the last offset
    length: O,
}

impl<O: Offset> BinaryPrimitive<O> {
    /// Initializes a new empty [`BinaryPrimitive`].
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Initializes a new [`BinaryPrimitive`] with a pre-allocated capacity of slots.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacities(capacity, 0)
    }

    /// Initializes a new [`BinaryPrimitive`] with a pre-allocated capacity of slots and values.
    pub fn with_capacities(capacity: usize, values: usize) -> Self {
        let mut offsets = MutableBuffer::<O>::with_capacity(capacity + 1);
        let length = O::default();
        offsets.push(length);

        Self {
            offsets,
            values: MutableBuffer::<u8>::with_capacity(values),
            validity: MutableBitmap::with_capacity(capacity),
            length,
        }
    }
}

impl<O: Offset> Default for BinaryPrimitive<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Offset, P: AsRef<[u8]>> FromIterator<Option<P>> for BinaryPrimitive<O> {
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        Self::try_from_iter(iter.into_iter()).unwrap()
    }
}

impl<O: Offset, P> TryFromIterator<Option<P>> for BinaryPrimitive<O>
where
    P: AsRef<[u8]>,
{
    fn try_from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> ArrowResult<Self> {
        let mut primitive = Self::new();
        primitive.try_extend(iter)?;
        Ok(primitive)
    }
}

impl<O: Offset, P> TryExtend<Option<P>> for BinaryPrimitive<O>
where
    P: AsRef<[u8]>,
{
    fn try_extend<I: IntoIterator<Item = Option<P>>>(&mut self, iter: I) -> ArrowResult<()> {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();
        self.validity.reserve(lower);
        self.offsets.reserve(lower);
        for item in iter {
            match item {
                Some(x) => self.try_push(x.as_ref())?,
                None => self.push_null(),
            }
        }
        Ok(())
    }
}

impl<O: Offset> NullableBuilder for BinaryPrimitive<O> {
    #[inline]
    fn push_null(&mut self) {
        self.offsets.push(self.length);
        self.validity.push(false);
    }
}

impl<O: Offset> Builder<&[u8]> for BinaryPrimitive<O> {
    #[inline]
    fn try_push(&mut self, value: &[u8]) -> ArrowResult<()> {
        let length = O::from_usize(value.len()).ok_or(ArrowError::DictionaryKeyOverflowError)?;
        self.length += length;
        self.offsets.push(self.length);
        self.values.extend_from_slice(value);
        self.validity.push(true);
        Ok(())
    }

    #[inline]
    fn push(&mut self, value: &[u8]) {
        self.try_push(value).unwrap()
    }
}

impl<O: Offset> BinaryPrimitive<O> {
    pub fn to(self) -> BinaryArray<O> {
        BinaryArray::<O>::from_data(
            self.offsets.into(),
            self.values.into(),
            self.validity.into(),
        )
    }
}

impl<O: Offset> ToArray for BinaryPrimitive<O> {
    fn to_arc(self, _: &DataType) -> Arc<dyn Array> {
        Arc::new(self.to())
    }
}

impl<O: Offset> IntoArray for BinaryPrimitive<O> {
    fn into_arc(self) -> Arc<dyn Array> {
        Arc::new(self.to())
    }
}

impl<O: Offset, P: AsRef<[u8]>> FromIterator<Option<P>> for BinaryArray<O> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        BinaryPrimitive::from_iter(iter).to()
    }
}

/// Creates [`Bitmap`] and two [`Buffer`]s from an iterator of `Option`.
/// The first buffer corresponds to a offset buffer, the second one
/// corresponds to a values buffer.
/// # Safety
/// The caller must ensure that `iterator` is `TrustedLen`.
#[inline]
pub(crate) unsafe fn trusted_len_unzip<O, I, P>(
    iterator: I,
) -> (Option<Bitmap>, Buffer<O>, Buffer<u8>)
where
    O: Offset,
    P: AsRef<[u8]>,
    I: Iterator<Item = Option<P>>,
{
    let (_, upper) = iterator.size_hint();
    let len = upper.expect("trusted_len_unzip requires an upper limit");

    let mut null = MutableBitmap::with_capacity(len);
    let mut offsets = MutableBuffer::<O>::with_capacity(len + 1);
    let mut values = MutableBuffer::<u8>::new();

    let mut length = O::default();
    let mut dst = offsets.as_mut_ptr();
    std::ptr::write(dst, length);
    dst = dst.add(1);
    for item in iterator {
        if let Some(item) = item {
            null.push(true);
            let s = item.as_ref();
            length += O::from_usize(s.len()).unwrap();
            values.extend_from_slice(s);
        } else {
            null.push(false);
            values.extend_from_slice(b"");
        };

        std::ptr::write(dst, length);
        dst = dst.add(1);
    }
    assert_eq!(
        dst.offset_from(offsets.as_ptr()) as usize,
        len + 1,
        "Trusted iterator length was not accurately reported"
    );
    offsets.set_len(len + 1);

    (null.into(), offsets.into(), values.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from() {
        let array = BinaryArray::<i32>::from(&[Some(b"hello".as_ref()), Some(b" ".as_ref()), None]);

        let a = array.validity().as_ref().unwrap();
        assert_eq!(a.len(), 3);
        assert_eq!(a.as_slice()[0], 0b00000011);
    }
}
