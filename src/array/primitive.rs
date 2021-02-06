use crate::{
    buffer::{types::NativeType, Bitmap, Buffer, MutableBitmap, MutableBuffer},
    datatypes::DataType,
};

use super::Array;

#[derive(Debug)]
pub struct PrimitiveArray<T: NativeType> {
    data_type: DataType,
    values: Buffer<T>,
    validity: Option<Bitmap>,
}

impl<T: NativeType> PrimitiveArray<T> {
    pub fn from_data(data_type: DataType, values: Buffer<T>, validity: Option<Bitmap>) -> Self {
        Self {
            data_type,
            values,
            validity,
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.as_ref().map(|x| x.slice(offset, length));
        Self {
            data_type: self.data_type.clone(),
            values: self.values.slice(offset, length),
            validity,
        }
    }

    #[inline]
    pub fn values(&self) -> &[T] {
        self.values.as_slice()
    }
}

impl<T: NativeType> Array for PrimitiveArray<T> {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn nulls(&self) -> &Option<Bitmap> {
        &self.validity
    }
}

impl<T: NativeType> From<(DataType, &[Option<T>])> for PrimitiveArray<T> {
    fn from((datatype, slice): (DataType, &[Option<T>])) -> Self {
        unsafe { Self::from_trusted_len_iter(datatype, slice.iter()) }
    }
}

impl<T: NativeType> PrimitiveArray<T> {
    /// Creates a [`PrimitiveArray`] from an iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn from_trusted_len_iter<I, P>(data_type: DataType, iter: I) -> Self
    where
        P: std::borrow::Borrow<Option<T>>,
        I: IntoIterator<Item = P>,
    {
        let iterator = iter.into_iter();

        let (validity, values) = trusted_len_unzip(iterator);

        Self::from_data(data_type, values, validity)
    }
}

/// Creates a Bitmap and a [`Buffer`] from an iterator of `Option`.
/// The first buffer corresponds to a bitmap buffer, the second one
/// corresponds to a values buffer.
/// # Safety
/// The caller must ensure that `iterator` is `TrustedLen`.
#[inline]
pub(crate) unsafe fn trusted_len_unzip<I, P, T>(iterator: I) -> (Option<Bitmap>, Buffer<T>)
where
    T: NativeType,
    P: std::borrow::Borrow<Option<T>>,
    I: Iterator<Item = P>,
{
    let (_, upper) = iterator.size_hint();
    let upper = upper.expect("trusted_len_unzip requires an upper limit");
    let len = upper * std::mem::size_of::<T>();

    let mut null = MutableBitmap::with_capacity(upper);
    let mut buffer = MutableBuffer::<T>::with_capacity(len);

    let mut dst = buffer.as_mut_ptr();
    for item in iterator {
        let item = item.borrow();
        if let Some(item) = item {
            std::ptr::write(dst, *item);
            null.push_unchecked(true);
        } else {
            null.push_unchecked(false);
            std::ptr::write(dst, T::default());
        }
        dst = dst.add(1);
    }
    assert_eq!(
        dst.offset_from(buffer.as_ptr() as *mut T) as usize,
        upper,
        "Trusted iterator length was not accurately reported"
    );
    buffer.set_len(len);
    null.set_len(len);

    let bitmap = if null.null_count() > 0 {
        Some(null.into())
    } else {
        None
    };
    (bitmap, buffer.into())
}
