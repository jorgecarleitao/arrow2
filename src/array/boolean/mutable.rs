use std::iter::FromIterator;
use std::sync::Arc;

use crate::array::TryExtend;
use crate::{
    array::{Array, MutableArray},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::Result,
    trusted_len::TrustedLen,
};

use super::BooleanArray;

/// The mutable version of [`BooleanArray`]. See [`MutableArray`] for more details.
#[derive(Debug)]
pub struct MutableBooleanArray {
    values: MutableBitmap,
    validity: Option<MutableBitmap>,
}

impl From<MutableBooleanArray> for BooleanArray {
    fn from(other: MutableBooleanArray) -> Self {
        BooleanArray::from_data(other.values.into(), other.validity.map(|x| x.into()))
    }
}

impl Default for MutableBooleanArray {
    fn default() -> Self {
        Self::new()
    }
}

impl MutableBooleanArray {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: MutableBitmap::with_capacity(capacity),
            validity: None,
        }
    }

    pub fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
        if let Some(x) = self.validity.as_mut() {
            x.reserve(additional)
        }
    }

    pub fn from_data(values: MutableBitmap, validity: Option<MutableBitmap>) -> Self {
        Self { values, validity }
    }

    pub fn push(&mut self, value: Option<bool>) {
        match value {
            Some(value) => {
                self.values.push(value);
                match &mut self.validity {
                    Some(validity) => validity.push(true),
                    None => {}
                }
            }
            None => {
                self.values.push(false);
                match &mut self.validity {
                    Some(validity) => validity.push(false),
                    None => self.init_validity(),
                }
            }
        }
    }

    fn init_validity(&mut self) {
        self.validity = Some(MutableBitmap::from_trusted_len_iter(
            std::iter::repeat(true)
                .take(self.len() - 1)
                .chain(std::iter::once(false)),
        ))
    }

    /// Converts itself into an [`Array`].
    pub fn into_arc(self) -> Arc<dyn Array> {
        let a: BooleanArray = self.into();
        Arc::new(a)
    }
}

/// Getters
impl MutableBooleanArray {
    /// Returns its values.
    pub fn values(&self) -> &MutableBitmap {
        &self.values
    }
}

/// Setters
impl MutableBooleanArray {
    /// Sets position `index` to `value`.
    /// Note that if it is the first time a null appears in this array,
    /// this initializes the validity bitmap (`O(N)`).
    /// # Panic
    /// Panics iff index is larger than `self.len()`.
    pub fn set(&mut self, index: usize, value: Option<bool>) {
        self.values.set(index, value.unwrap_or_default());

        if value.is_none() && self.validity.is_none() {
            // When the validity is None, all elements so far are valid. When one of the elements is set fo null,
            // the validity must be initialized.
            self.validity = Some(MutableBitmap::from_trusted_len_iter(
                std::iter::repeat(true).take(self.len()),
            ));
        }
        if let Some(x) = self.validity.as_mut() {
            x.set(index, value.is_some())
        }
    }
}

/// From implementations
impl MutableBooleanArray {
    /// Creates a new [`MutableBooleanArray`] from an [`TrustedLen`] of `bool`.
    #[inline]
    pub fn from_trusted_len_values_iter<I: TrustedLen<Item = bool>>(iterator: I) -> Self {
        Self::from_data(MutableBitmap::from_trusted_len_iter(iterator), None)
    }

    /// Creates a new [`MutableBooleanArray`] from a slice of `bool`.
    #[inline]
    pub fn from_slice<P: AsRef<[bool]>>(slice: P) -> Self {
        Self::from_trusted_len_values_iter(slice.as_ref().iter().copied())
    }

    /// Creates a [`BooleanArray`] from an iterator of trusted length.
    /// Use this over [`BooleanArray::from_trusted_len_iter`] when the iterator is trusted len
    /// but this crate does not mark it as such.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn from_trusted_len_iter_unchecked<I, P>(iterator: I) -> Self
    where
        P: std::borrow::Borrow<bool>,
        I: Iterator<Item = Option<P>>,
    {
        let (validity, values) = trusted_len_unzip(iterator);

        let validity = if validity.null_count() > 0 {
            Some(validity)
        } else {
            None
        };

        Self::from_data(values, validity)
    }

    /// Creates a [`BooleanArray`] from a [`TrustedLen`].
    #[inline]
    pub fn from_trusted_len_iter<I, P>(iterator: I) -> Self
    where
        P: std::borrow::Borrow<bool>,
        I: TrustedLen<Item = Option<P>>,
    {
        let (validity, values) = unsafe { trusted_len_unzip(iterator) };

        let validity = if validity.null_count() > 0 {
            Some(validity)
        } else {
            None
        };

        Self::from_data(values, validity)
    }

    /// Creates a [`BooleanArray`] from an falible iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn try_from_trusted_len_iter_unchecked<E, I, P>(
        iterator: I,
    ) -> std::result::Result<Self, E>
    where
        P: std::borrow::Borrow<bool>,
        I: Iterator<Item = std::result::Result<Option<P>, E>>,
    {
        let (validity, values) = try_trusted_len_unzip(iterator)?;

        let validity = if validity.null_count() > 0 {
            Some(validity)
        } else {
            None
        };

        Ok(Self::from_data(values, validity))
    }

    /// Creates a [`BooleanArray`] from a [`TrustedLen`].
    #[inline]
    pub fn try_from_trusted_len_iter<E, I, P>(iterator: I) -> std::result::Result<Self, E>
    where
        P: std::borrow::Borrow<bool>,
        I: TrustedLen<Item = std::result::Result<Option<P>, E>>,
    {
        let (validity, values) = unsafe { try_trusted_len_unzip(iterator)? };

        let validity = if validity.null_count() > 0 {
            Some(validity)
        } else {
            None
        };

        Ok(Self::from_data(values, validity))
    }
}

/// Creates a Bitmap and an optional [`MutableBitmap`] from an iterator of `Option<bool>`.
/// The first buffer corresponds to a bitmap buffer, the second one
/// corresponds to a values buffer.
/// # Safety
/// The caller must ensure that `iterator` is `TrustedLen`.
#[inline]
pub(crate) unsafe fn trusted_len_unzip<I, P>(iterator: I) -> (MutableBitmap, MutableBitmap)
where
    P: std::borrow::Borrow<bool>,
    I: Iterator<Item = Option<P>>,
{
    let (_, upper) = iterator.size_hint();
    let len = upper.expect("trusted_len_unzip requires an upper limit");

    let mut validity = MutableBitmap::with_capacity(len);
    let mut values = MutableBitmap::with_capacity(len);

    for item in iterator {
        let item = if let Some(item) = item {
            validity.push(true);
            *item.borrow()
        } else {
            validity.push(false);
            false
        };
        values.push(item);
    }
    assert_eq!(
        values.len(),
        len,
        "Trusted iterator length was not accurately reported"
    );
    (validity, values)
}

/// # Safety
/// The caller must ensure that `iterator` is `TrustedLen`.
#[inline]
pub(crate) unsafe fn try_trusted_len_unzip<E, I, P>(
    iterator: I,
) -> std::result::Result<(MutableBitmap, MutableBitmap), E>
where
    P: std::borrow::Borrow<bool>,
    I: Iterator<Item = std::result::Result<Option<P>, E>>,
{
    let (_, upper) = iterator.size_hint();
    let len = upper.expect("trusted_len_unzip requires an upper limit");

    let mut null = MutableBitmap::with_capacity(len);
    let mut values = MutableBitmap::with_capacity(len);

    for item in iterator {
        let item = if let Some(item) = item? {
            null.push(true);
            *item.borrow()
        } else {
            null.push(false);
            false
        };
        values.push(item);
    }
    assert_eq!(
        values.len(),
        len,
        "Trusted iterator length was not accurately reported"
    );
    values.set_len(len);
    null.set_len(len);

    Ok((null, values))
}

impl<Ptr: std::borrow::Borrow<Option<bool>>> FromIterator<Ptr> for MutableBooleanArray {
    fn from_iter<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();

        let mut validity = MutableBitmap::with_capacity(lower);

        let values: MutableBitmap = iter
            .map(|item| {
                if let Some(a) = item.borrow() {
                    validity.push(true);
                    *a
                } else {
                    validity.push(false);
                    false
                }
            })
            .collect();

        MutableBooleanArray::from_data(values, validity.into())
    }
}

impl MutableArray for MutableBooleanArray {
    fn len(&self) -> usize {
        self.values.len()
    }

    fn validity(&self) -> &Option<MutableBitmap> {
        &self.validity
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(BooleanArray::from_data(
            std::mem::take(&mut self.values).into(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        ))
    }

    fn data_type(&self) -> &DataType {
        &DataType::Boolean
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    #[inline]
    fn push_null(&mut self) {
        self.push(None)
    }
}

impl Extend<Option<bool>> for MutableBooleanArray {
    fn extend<I: IntoIterator<Item = Option<bool>>>(&mut self, iter: I) {
        let iter = iter.into_iter();
        self.reserve(iter.size_hint().0);
        iter.for_each(|x| self.push(x))
    }
}

impl TryExtend<Option<bool>> for MutableBooleanArray {
    /// This is infalible and is implemented for consistency with all other types
    fn try_extend<I: IntoIterator<Item = Option<bool>>>(&mut self, iter: I) -> Result<()> {
        self.extend(iter);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push() {
        let mut a = MutableBooleanArray::new();
        a.push(Some(true));
        a.push(None);
        assert_eq!(a.len(), 2);
        assert!(a.is_valid(0));
        assert!(!a.is_valid(1));

        assert_eq!(a.values(), &MutableBitmap::from([true, false]));
    }

    #[test]
    fn from_trusted_len_iter() {
        let iter = std::iter::repeat(true).take(2).map(Some);
        let a = MutableBooleanArray::from_trusted_len_iter(iter);
        assert_eq!(a.len(), 2);
    }

    #[test]
    fn from_iter() {
        let iter = std::iter::repeat(true).take(2).map(Some);
        let a = MutableBooleanArray::from_iter(iter);
        assert_eq!(a.len(), 2);
    }
}
