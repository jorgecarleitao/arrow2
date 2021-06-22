use std::{iter::FromIterator, sync::Arc};

use crate::{
    array::{Array, MutableArray, TryExtend},
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
    datatypes::DataType,
    error::Result,
    trusted_len::TrustedLen,
    types::{NativeType, NaturalDataType},
};

use super::PrimitiveArray;

/// The mutable version of [`PrimitiveArray`]. See [`MutableArray`] for more details.
#[derive(Debug)]
pub struct MutablePrimitiveArray<T: NativeType> {
    data_type: DataType,
    values: MutableBuffer<T>,
    validity: Option<MutableBitmap>,
}

impl<T: NativeType> From<MutablePrimitiveArray<T>> for PrimitiveArray<T> {
    fn from(other: MutablePrimitiveArray<T>) -> Self {
        let validity = if other.validity.as_ref().map(|x| x.null_count()).unwrap_or(0) > 0 {
            other.validity.map(|x| x.into())
        } else {
            None
        };

        PrimitiveArray::<T>::from_data(other.data_type, other.values.into(), validity)
    }
}

impl<T: NativeType + NaturalDataType, P: AsRef<[Option<T>]>> From<P> for MutablePrimitiveArray<T> {
    fn from(slice: P) -> Self {
        Self::from_trusted_len_iter(slice.as_ref().iter().map(|x| x.as_ref()))
    }
}

impl<T: NativeType + NaturalDataType> MutablePrimitiveArray<T> {
    /// Creates a new empty [`MutablePrimitiveArray`].
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Creates a new [`MutablePrimitiveArray`] with a capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_from(capacity, T::DATA_TYPE)
    }
}

impl<T: NativeType + NaturalDataType> Default for MutablePrimitiveArray<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: NativeType> From<DataType> for MutablePrimitiveArray<T> {
    fn from(data_type: DataType) -> Self {
        assert!(T::is_valid(&data_type));
        Self {
            data_type,
            values: MutableBuffer::<T>::new(),
            validity: None,
        }
    }
}

impl<T: NativeType> MutablePrimitiveArray<T> {
    /// Creates a new [`MutablePrimitiveArray`] from a capacity and [`DataType`].
    pub fn with_capacity_from(capacity: usize, data_type: DataType) -> Self {
        assert!(T::is_valid(&data_type));
        Self {
            data_type,
            values: MutableBuffer::<T>::with_capacity(capacity),
            validity: None,
        }
    }

    /// Reserves `additional` entries.
    pub fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
        if let Some(x) = self.validity.as_mut() {
            x.reserve(additional)
        }
    }

    /// Adds a new value to the array.
    pub fn push(&mut self, value: Option<T>) {
        match value {
            Some(value) => {
                self.values.push(value);
                match &mut self.validity {
                    Some(validity) => validity.push(true),
                    None => {
                        self.init_validity();
                    }
                }
            }
            None => {
                self.values.push(T::default());
                match &mut self.validity {
                    Some(validity) => validity.push(false),
                    None => {}
                }
            }
        }
    }

    fn init_validity(&mut self) {
        self.validity = Some(MutableBitmap::from_trusted_len_iter(
            std::iter::repeat(false)
                .take(self.len() - 1)
                .chain(std::iter::once(true)),
        ))
    }

    /// Changes the arrays' [`DataType`], returning a new [`MutablePrimitiveArray`].
    /// Use to change the logical type without changing the corresponding physical Type.
    /// # Implementation
    /// This operation is `O(1)`.
    pub fn to(self, data_type: DataType) -> Self {
        assert!(T::is_valid(&data_type));
        Self {
            data_type,
            values: self.values,
            validity: self.validity,
        }
    }

    /// Converts itself into an [`Array`].
    pub fn into_arc(self) -> Arc<dyn Array> {
        let a: PrimitiveArray<T> = self.into();
        Arc::new(a)
    }
}

/// Accessors
impl<T: NativeType> MutablePrimitiveArray<T> {
    /// Returns its values.
    pub fn values(&self) -> &MutableBuffer<T> {
        &self.values
    }

    /// Mutable slice of values
    pub fn values_mut_slice(&mut self) -> &mut [T] {
        self.values.as_mut_slice()
    }
}

/// Setters
impl<T: NativeType> MutablePrimitiveArray<T> {
    /// Sets position `index` to `value`.
    /// Note that if it is the first time a null appears in this array,
    /// this initializes the validity bitmap (`O(N)`).
    /// # Panic
    /// Panics iff index is larger than `self.len()`.
    pub fn set(&mut self, index: usize, value: Option<T>) {
        self.values[index] = value.unwrap_or_default();

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

    pub fn set_validity(&mut self, validity: Option<MutableBitmap>) {
        if let Some(validity) = &validity {
            assert_eq!(self.values.len(), validity.len())
        }
        self.validity = validity;
    }

    pub fn set_values(&mut self, values: MutableBuffer<T>) {
        assert_eq!(values.len(), self.values.len());
        self.values = values;
    }
}

impl<T: NativeType> Extend<Option<T>> for MutablePrimitiveArray<T> {
    fn extend<I: IntoIterator<Item = Option<T>>>(&mut self, iter: I) {
        let iter = iter.into_iter();
        self.reserve(iter.size_hint().0);
        iter.for_each(|x| self.push(x))
    }
}

impl<T: NativeType> TryExtend<Option<T>> for MutablePrimitiveArray<T> {
    /// This is infalible and is implemented for consistency with all other types
    fn try_extend<I: IntoIterator<Item = Option<T>>>(&mut self, iter: I) -> Result<()> {
        self.extend(iter);
        Ok(())
    }
}

impl<T: NativeType> MutableArray for MutablePrimitiveArray<T> {
    fn len(&self) -> usize {
        self.values.len()
    }

    fn validity(&self) -> &Option<MutableBitmap> {
        &self.validity
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(PrimitiveArray::from_data(
            self.data_type.clone(),
            std::mem::take(&mut self.values).into(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        ))
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn push_null(&mut self) {
        self.push(None)
    }
}

impl<T: NativeType + NaturalDataType> MutablePrimitiveArray<T> {
    /// Creates a [`MutablePrimitiveArray`] from a slice of values.
    pub fn from_slice<P: AsRef<[T]>>(slice: P) -> Self {
        Self::from_trusted_len_values_iter(slice.as_ref().iter().copied())
    }

    /// Creates a [`MutablePrimitiveArray`] from an iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn from_trusted_len_iter_unchecked<I, P>(iterator: I) -> Self
    where
        P: std::borrow::Borrow<T>,
        I: Iterator<Item = Option<P>>,
    {
        let (validity, values) = trusted_len_unzip(iterator);

        Self {
            data_type: T::DATA_TYPE,
            values,
            validity,
        }
    }

    /// Creates a [`MutablePrimitiveArray`] from a [`TrustedLen`].
    #[inline]
    pub fn from_trusted_len_iter<I, P>(iterator: I) -> Self
    where
        P: std::borrow::Borrow<T>,
        I: TrustedLen<Item = Option<P>>,
    {
        let (validity, values) = unsafe { trusted_len_unzip(iterator) };

        Self {
            data_type: T::DATA_TYPE,
            values,
            validity,
        }
    }

    /// Creates a [`MutablePrimitiveArray`] from an fallible iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn try_from_trusted_len_iter_unchecked<E, I, P>(
        iter: I,
    ) -> std::result::Result<Self, E>
    where
        P: std::borrow::Borrow<T>,
        I: IntoIterator<Item = std::result::Result<Option<P>, E>>,
    {
        let iterator = iter.into_iter();

        let (validity, values) = try_trusted_len_unzip(iterator)?;

        Ok(Self {
            data_type: T::DATA_TYPE,
            values,
            validity,
        })
    }

    /// Creates a [`MutablePrimitiveArray`] from an fallible iterator of trusted length.
    #[inline]
    pub fn try_from_trusted_len_iter<E, I, P>(iterator: I) -> std::result::Result<Self, E>
    where
        P: std::borrow::Borrow<T>,
        I: TrustedLen<Item = std::result::Result<Option<P>, E>>,
    {
        let (validity, values) = unsafe { try_trusted_len_unzip(iterator) }?;

        Ok(Self {
            data_type: T::DATA_TYPE,
            values,
            validity,
        })
    }

    /// Creates a new [`MutablePrimitiveArray`] out an iterator over values
    pub fn from_trusted_len_values_iter<I: TrustedLen<Item = T>>(iter: I) -> Self {
        Self {
            data_type: T::DATA_TYPE,
            values: MutableBuffer::<T>::from_trusted_len_iter(iter),
            validity: None,
        }
    }

    /// Creates a new [`MutablePrimitiveArray`] from an iterator over values
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    pub unsafe fn from_trusted_len_values_iter_unchecked<I: Iterator<Item = T>>(iter: I) -> Self {
        Self {
            data_type: T::DATA_TYPE,
            values: MutableBuffer::<T>::from_trusted_len_iter_unchecked(iter),
            validity: None,
        }
    }
}

impl<T: NativeType + NaturalDataType, Ptr: std::borrow::Borrow<Option<T>>> FromIterator<Ptr>
    for MutablePrimitiveArray<T>
{
    fn from_iter<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();

        let mut validity = MutableBitmap::with_capacity(lower);

        let values: MutableBuffer<T> = iter
            .map(|item| {
                if let Some(a) = item.borrow() {
                    validity.push(true);
                    *a
                } else {
                    validity.push(false);
                    T::default()
                }
            })
            .collect();

        let validity = if validity.null_count() > 0 {
            Some(validity)
        } else {
            None
        };

        Self {
            data_type: T::DATA_TYPE,
            values,
            validity,
        }
    }
}

/// Creates a [`MutableBitmap`] and a [`MutableBuffer`] from an iterator of `Option`.
/// The first buffer corresponds to a bitmap buffer, the second one
/// corresponds to a values buffer.
/// # Safety
/// The caller must ensure that `iterator` is `TrustedLen`.
#[inline]
pub(crate) unsafe fn trusted_len_unzip<I, P, T>(
    iterator: I,
) -> (Option<MutableBitmap>, MutableBuffer<T>)
where
    T: NativeType,
    P: std::borrow::Borrow<T>,
    I: Iterator<Item = Option<P>>,
{
    let (_, upper) = iterator.size_hint();
    let len = upper.expect("trusted_len_unzip requires an upper limit");

    let mut validity = MutableBitmap::with_capacity(len);
    let mut buffer = MutableBuffer::<T>::with_capacity(len);

    let mut dst = buffer.as_mut_ptr();
    for item in iterator {
        let item = if let Some(item) = item {
            validity.push_unchecked(true);
            *item.borrow()
        } else {
            validity.push_unchecked(false);
            T::default()
        };
        std::ptr::write(dst, item);
        dst = dst.add(1);
    }
    assert_eq!(
        dst.offset_from(buffer.as_ptr()) as usize,
        len,
        "Trusted iterator length was not accurately reported"
    );
    buffer.set_len(len);

    let validity = if validity.null_count() > 0 {
        Some(validity)
    } else {
        None
    };

    (validity, buffer)
}

/// # Safety
/// The caller must ensure that `iterator` is `TrustedLen`.
#[inline]
pub(crate) unsafe fn try_trusted_len_unzip<E, I, P, T>(
    iterator: I,
) -> std::result::Result<(Option<MutableBitmap>, MutableBuffer<T>), E>
where
    T: NativeType,
    P: std::borrow::Borrow<T>,
    I: Iterator<Item = std::result::Result<Option<P>, E>>,
{
    let (_, upper) = iterator.size_hint();
    let len = upper.expect("trusted_len_unzip requires an upper limit");

    let mut null = MutableBitmap::with_capacity(len);
    let mut buffer = MutableBuffer::<T>::with_capacity(len);

    let mut dst = buffer.as_mut_ptr();
    for item in iterator {
        let item = if let Some(item) = item? {
            null.push(true);
            *item.borrow()
        } else {
            null.push(false);
            T::default()
        };
        std::ptr::write(dst, item);
        dst = dst.add(1);
    }
    assert_eq!(
        dst.offset_from(buffer.as_ptr()) as usize,
        len,
        "Trusted iterator length was not accurately reported"
    );
    buffer.set_len(len);
    null.set_len(len);

    let validity = if null.null_count() > 0 {
        Some(null)
    } else {
        None
    };

    Ok((validity, buffer))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push() {
        let mut a = MutablePrimitiveArray::<i32>::new();
        a.push(Some(1));
        a.push(None);
        assert_eq!(a.len(), 2);
        assert!(a.is_valid(0));
        assert!(!a.is_valid(1));

        assert_eq!(a.values(), &MutableBuffer::from([1, 0]));
    }

    #[test]
    fn set() {
        let mut a = MutablePrimitiveArray::<i32>::from([Some(1), None]);

        a.set(0, Some(2));
        a.set(1, Some(1));

        assert_eq!(a.len(), 2);
        assert!(a.is_valid(0));
        assert!(a.is_valid(1));

        assert_eq!(a.values(), &MutableBuffer::from([2, 1]));
    }

    #[test]
    fn from_iter() {
        let a = MutablePrimitiveArray::<i32>::from_iter((0..2).map(Some));
        assert_eq!(a.len(), 2);
        assert_eq!(a.validity(), &None);
    }

    #[test]
    fn natural_arc() {
        let a = MutablePrimitiveArray::<i32>::from_slice(&[0, 1]).into_arc();
        assert_eq!(a.len(), 2);
    }
}
