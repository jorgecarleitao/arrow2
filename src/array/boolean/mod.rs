use crate::{
    buffer::{Bitmap, MutableBitmap},
    datatypes::DataType,
    ffi::ArrowArray,
};

use super::{ffi::ToFFI, Array, FromFFI};

use crate::error::Result;

#[derive(Debug)]
pub struct BooleanArray {
    data_type: DataType,
    values: Bitmap,
    validity: Option<Bitmap>,
    offset: usize,
}

impl BooleanArray {
    pub fn new_empty() -> Self {
        Self::from_data(Bitmap::new(), None)
    }

    pub fn from_data(values: Bitmap, validity: Option<Bitmap>) -> Self {
        Self {
            data_type: DataType::Boolean,
            values,
            validity,
            offset: 0,
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.clone().map(|x| x.slice(offset, length));
        Self {
            data_type: self.data_type.clone(),
            values: self.values.clone().slice(offset, length),
            validity,
            offset: self.offset + offset,
        }
    }

    /// Returns the element at index `i` as &str
    /// # Safety
    /// Assumes that the `i < self.len`.
    pub fn value(&self, i: usize) -> bool {
        self.values.get_bit(i)
    }
}

impl Array for BooleanArray {
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

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

unsafe impl ToFFI for BooleanArray {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3] {
        [
            self.validity.as_ref().map(|x| x.as_ptr()),
            Some(self.values.as_ptr()),
            None,
        ]
    }

    fn offset(&self) -> usize {
        self.offset
    }
}

unsafe impl FromFFI for BooleanArray {
    fn try_from_ffi(data_type: DataType, array: ArrowArray) -> Result<Self> {
        let length = array.len();
        let offset = array.offset();
        let mut validity = array.null_bit_buffer();
        let mut values = unsafe { array.bitmap(0)? };

        if offset > 0 {
            values = values.slice(offset, length);
            validity = validity.map(|x| x.slice(offset, length))
        }
        Ok(Self {
            data_type,
            values,
            validity,
            offset: 0,
        })
    }
}

impl<P: AsRef<[Option<bool>]>> From<P> for BooleanArray {
    fn from(slice: P) -> Self {
        unsafe { Self::from_trusted_len_iter(slice.as_ref().iter().map(|x| x.as_ref())) }
    }
}

impl BooleanArray {
    pub fn from_slice<P: AsRef<[bool]>>(slice: P) -> Self {
        unsafe { Self::from_trusted_len_iter(slice.as_ref().iter().map(Some)) }
    }
}

impl BooleanArray {
    /// Creates a [`BooleanArray`] from an iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn from_trusted_len_iter<I, P>(iter: I) -> Self
    where
        P: std::borrow::Borrow<bool>,
        I: IntoIterator<Item = Option<P>>,
    {
        let iterator = iter.into_iter();

        let (validity, values) = trusted_len_unzip(iterator);

        Self::from_data(values, validity)
    }
}

/// Creates a Bitmap and a [`Buffer`] from an iterator of `Option`.
/// The first buffer corresponds to a bitmap buffer, the second one
/// corresponds to a values buffer.
/// # Safety
/// The caller must ensure that `iterator` is `TrustedLen`.
#[inline]
pub(crate) unsafe fn trusted_len_unzip<I, P>(iterator: I) -> (Option<Bitmap>, Bitmap)
where
    P: std::borrow::Borrow<bool>,
    I: Iterator<Item = Option<P>>,
{
    let (_, upper) = iterator.size_hint();
    let len = upper.expect("trusted_len_unzip requires an upper limit");

    let mut null = MutableBitmap::with_capacity(len);
    let mut values = MutableBitmap::with_capacity(len);

    for item in iterator {
        let item = if let Some(item) = item {
            null.push_unchecked(true);
            *item.borrow()
        } else {
            null.push_unchecked(false);
            false
        };
        values.push_unchecked(item);
    }
    assert_eq!(
        values.len(),
        len,
        "Trusted iterator length was not accurately reported"
    );
    values.set_len(len);
    null.set_len(len);

    let bitmap = if null.null_count() > 0 {
        Some(null.into())
    } else {
        None
    };
    (bitmap, values.into())
}

mod iterator;
pub use iterator::*;
