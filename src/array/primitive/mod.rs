use crate::{
    bitmap::{
        utils::{zip_validity, ZipValidity},
        Bitmap,
    },
    buffer::Buffer,
    datatypes::*,
    error::Error,
    trusted_len::TrustedLen,
    types::{days_ms, months_days_ns, NativeType},
};

use super::Array;
use either::Either;

mod ffi;
pub(super) mod fmt;
mod from_natural;
mod iterator;
pub use iterator::*;
mod mutable;
pub use mutable::*;

/// A [`PrimitiveArray`] is Arrow's semantically equivalent of an immutable `Vec<Option<T>>` where
/// T is [`NativeType`] (e.g. [`i32`]). It implements [`Array`].
///
/// One way to think about a [`PrimitiveArray`] is `(DataType, Arc<Vec<T>>, Option<Arc<Vec<u8>>>)`
/// where:
/// * the first item is the array's logical type
/// * the second is the immutable values
/// * the third is the immutable validity (whether a value is null or not as a bitmap).
///
/// The size of this struct is `O(1)`, as all data is stored behind an [`std::sync::Arc`].
/// # Example
/// ```
/// use arrow2::array::PrimitiveArray;
/// use arrow2::bitmap::Bitmap;
/// use arrow2::buffer::Buffer;
///
/// let array = PrimitiveArray::from([Some(1i32), None, Some(10)]);
/// assert_eq!(array.value(0), 1);
/// assert_eq!(array.iter().collect::<Vec<_>>(), vec![Some(&1i32), None, Some(&10)]);
/// assert_eq!(array.values_iter().copied().collect::<Vec<_>>(), vec![1, 0, 10]);
/// // the underlying representation
/// assert_eq!(array.values(), &Buffer::from(vec![1i32, 0, 10]));
/// assert_eq!(array.validity(), Some(&Bitmap::from([true, false, true])));
///
/// ```
#[derive(Clone)]
pub struct PrimitiveArray<T: NativeType> {
    data_type: DataType,
    values: Buffer<T>,
    validity: Option<Bitmap>,
}

impl<T: NativeType> PrimitiveArray<T> {
    /// The canonical method to create a [`PrimitiveArray`] out of its internal components.
    /// # Implementation
    /// This function is `O(1)`.
    ///
    /// # Errors
    /// This function errors iff:
    /// * The validity is not `None` and its length is different from `values`'s length
    /// * The `data_type`'s [`PhysicalType`] is not equal to [`PhysicalType::Primitive(T::PRIMITIVE)`]
    pub fn try_new(
        data_type: DataType,
        values: Buffer<T>,
        validity: Option<Bitmap>,
    ) -> Result<Self, Error> {
        if validity
            .as_ref()
            .map_or(false, |validity| validity.len() != values.len())
        {
            return Err(Error::oos(
                "validity mask length must match the number of values",
            ));
        }

        if data_type.to_physical_type() != PhysicalType::Primitive(T::PRIMITIVE) {
            return Err(Error::oos(
                "BooleanArray can only be initialized with a DataType whose physical type is Primitive",
            ));
        }

        Ok(Self {
            data_type,
            values,
            validity,
        })
    }

    /// Returns a new [`PrimitiveArray`] with a different logical type.
    ///
    /// This function is useful to assign a different [`DataType`] to the array.
    /// Used to change the arrays' logical type (see example).
    /// # Example
    /// ```
    /// use arrow2::array::Int32Array;
    /// use arrow2::datatypes::DataType;
    ///
    /// let array = Int32Array::from(&[Some(1), None, Some(2)]).to(DataType::Date32);
    /// assert_eq!(
    ///    format!("{:?}", array),
    ///    "Date32[1970-01-02, None, 1970-01-03]"
    /// );
    /// ```
    /// # Panics
    /// Panics iff the `data_type`'s [`PhysicalType`] is not equal to [`PhysicalType::Primitive(T::PRIMITIVE)`]
    #[inline]
    #[must_use]
    pub fn to(self, data_type: DataType) -> Self {
        if !data_type.to_physical_type().eq_primitive(T::PRIMITIVE) {
            Err(Error::InvalidArgumentError(format!(
                "Type {} does not support logical type {:?}",
                std::any::type_name::<T>(),
                data_type
            )))
            .unwrap()
        }
        Self {
            data_type,
            values: self.values,
            validity: self.validity,
        }
    }

    /// Creates a (non-null) [`PrimitiveArray`] from a vector of values.
    /// # Examples
    /// ```
    /// use arrow2::array::PrimitiveArray;
    ///
    /// let array = PrimitiveArray::from_vec(vec![1, 2, 3]);
    /// assert_eq!(format!("{:?}", array), "Int32[1, 2, 3]");
    /// ```
    pub fn from_vec(values: Vec<T>) -> Self {
        Self::new(T::PRIMITIVE.into(), values.into(), None)
    }

    /// Returns an iterator over the values and validity, `Option<&T>`.
    #[inline]
    pub fn iter(&self) -> ZipValidity<&T, std::slice::Iter<T>> {
        zip_validity(
            self.values().iter(),
            self.validity().as_ref().map(|x| x.iter()),
        )
    }

    /// Returns an iterator of the values, `&T`, ignoring the arrays' validity.
    #[inline]
    pub fn values_iter(&self) -> std::slice::Iter<T> {
        self.values().iter()
    }

    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// The values [`Buffer`].
    /// Values on null slots are undetermined (they can be anything).
    #[inline]
    pub fn values(&self) -> &Buffer<T> {
        &self.values
    }

    /// Returns the optional validity.
    #[inline]
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// Returns the arrays' [`DataType`].
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the value at slot `i`.
    ///
    /// Equivalent to `self.values()[i]`. The value of a null slot is undetermined (it can be anything).
    /// # Panic
    /// This function panics iff `i >= self.len`.
    #[inline]
    pub fn value(&self, i: usize) -> T {
        self.values()[i]
    }

    /// Returns the value at index `i`.
    /// The value on null slots is undetermined (it can be anything).
    /// # Safety
    /// Caller must be sure that `i < self.len()`
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> T {
        *self.values.get_unchecked(i)
    }

    /// Returns a clone of this [`PrimitiveArray`] sliced by an offset and length.
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Examples
    /// ```
    /// use arrow2::array::PrimitiveArray;
    ///
    /// let array = PrimitiveArray::from_vec(vec![1, 2, 3]);
    /// assert_eq!(format!("{:?}", array), "Int32[1, 2, 3]");
    /// let sliced = array.slice(1, 1);
    /// assert_eq!(format!("{:?}", sliced), "Int32[2]");
    /// // note: `sliced` and `array` share the same memory region.
    /// ```
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    #[must_use]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Returns a clone of this [`PrimitiveArray`] sliced by an offset and length.
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    #[inline]
    #[must_use]
    pub unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        let validity = self
            .validity
            .clone()
            .map(|x| x.slice_unchecked(offset, length));
        Self {
            data_type: self.data_type.clone(),
            values: self.values.clone().slice_unchecked(offset, length),
            validity,
        }
    }

    /// Returns a clone of this [`PrimitiveArray`] with a new validity.
    /// # Panics
    /// This function panics iff `validity.len() != self.len()`.
    #[must_use]
    pub fn with_validity(&self, validity: Option<Bitmap>) -> Self {
        if matches!(&validity, Some(bitmap) if bitmap.len() != self.len()) {
            panic!("validity should be as least as large as the array")
        }
        let mut arr = self.clone();
        arr.validity = validity;
        arr
    }

    /// Try to convert this [`PrimitiveArray`] to a [`MutablePrimitiveArray`] via copy-on-write semantics.
    ///
    /// A [`PrimitiveArray`] is backed by a [`Buffer`] and [`Bitmap`] which are essentially `Arc<Vec<_>>`.
    /// This function returns a [`MutablePrimitiveArray`] (via [`std::sync::Arc::get_mut`]) iff both values
    /// and validity have not been cloned / are unique references to their underlying vectors.
    ///
    /// This function is primarily used to re-use memory regions.
    #[must_use]
    pub fn into_mut(self) -> Either<Self, MutablePrimitiveArray<T>> {
        use Either::*;

        if let Some(bitmap) = self.validity {
            match bitmap.into_mut() {
                Left(bitmap) => Left(PrimitiveArray::new(
                    self.data_type,
                    self.values,
                    Some(bitmap),
                )),
                Right(mutable_bitmap) => match self.values.into_mut() {
                    Left(buffer) => Left(PrimitiveArray::new(
                        self.data_type,
                        buffer,
                        Some(mutable_bitmap.into()),
                    )),
                    Right(values) => Right(MutablePrimitiveArray::from_data(
                        self.data_type,
                        values,
                        Some(mutable_bitmap),
                    )),
                },
            }
        } else {
            match self.values.into_mut() {
                Left(values) => Left(PrimitiveArray::new(self.data_type, values, None)),
                Right(values) => Right(MutablePrimitiveArray::from_data(
                    self.data_type,
                    values,
                    None,
                )),
            }
        }
    }

    /// Returns a new empty (zero-length) [`PrimitiveArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        Self::new(data_type, Buffer::new(), None)
    }

    /// Returns a new [`PrimitiveArray`] where all slots are null / `None`.
    #[inline]
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        Self::new(
            data_type,
            Buffer::new_zeroed(length),
            Some(Bitmap::new_zeroed(length)),
        )
    }

    /// Creates a (non-null) [`PrimitiveArray`] from an iterator of values.
    /// # Implementation
    /// This does not assume that the iterator has a known length.
    pub fn from_values<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::new(T::PRIMITIVE.into(), Vec::<T>::from_iter(iter).into(), None)
    }

    /// Creates a (non-null) [`PrimitiveArray`] from a slice of values.
    /// # Implementation
    /// This is essentially a memcopy and is thus `O(N)`
    pub fn from_slice<P: AsRef<[T]>>(slice: P) -> Self {
        Self::new(
            T::PRIMITIVE.into(),
            Vec::<T>::from(slice.as_ref()).into(),
            None,
        )
    }

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

    /// Alias for `Self::try_new(..).unwrap()`.
    /// # Panics
    /// This function errors iff:
    /// * The validity is not `None` and its length is different from `values`'s length
    /// * The `data_type`'s [`PhysicalType`] is not equal to [`PhysicalType::Primitive`].
    pub fn new(data_type: DataType, values: Buffer<T>, validity: Option<Bitmap>) -> Self {
        Self::try_new(data_type, values, validity).unwrap()
    }

    /// Alias for `Self::try_new(..).unwrap()`.
    pub fn from_data(data_type: DataType, values: Buffer<T>, validity: Option<Bitmap>) -> Self {
        Self::new(data_type, values, validity)
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
        self.data_type()
    }

    fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice_unchecked(offset, length))
    }
    fn with_validity(&self, validity: Option<Bitmap>) -> Box<dyn Array> {
        Box::new(self.with_validity(validity))
    }
    fn to_boxed(&self) -> Box<dyn Array> {
        Box::new(self.clone())
    }
}

/// A type definition [`PrimitiveArray`] for `i8`
pub type Int8Array = PrimitiveArray<i8>;
/// A type definition [`PrimitiveArray`] for `i16`
pub type Int16Array = PrimitiveArray<i16>;
/// A type definition [`PrimitiveArray`] for `i32`
pub type Int32Array = PrimitiveArray<i32>;
/// A type definition [`PrimitiveArray`] for `i64`
pub type Int64Array = PrimitiveArray<i64>;
/// A type definition [`PrimitiveArray`] for `i128`
pub type Int128Array = PrimitiveArray<i128>;
/// A type definition [`PrimitiveArray`] for [`days_ms`]
pub type DaysMsArray = PrimitiveArray<days_ms>;
/// A type definition [`PrimitiveArray`] for [`months_days_ns`]
pub type MonthsDaysNsArray = PrimitiveArray<months_days_ns>;
/// A type definition [`PrimitiveArray`] for `f32`
pub type Float32Array = PrimitiveArray<f32>;
/// A type definition [`PrimitiveArray`] for `f64`
pub type Float64Array = PrimitiveArray<f64>;
/// A type definition [`PrimitiveArray`] for `u8`
pub type UInt8Array = PrimitiveArray<u8>;
/// A type definition [`PrimitiveArray`] for `u16`
pub type UInt16Array = PrimitiveArray<u16>;
/// A type definition [`PrimitiveArray`] for `u32`
pub type UInt32Array = PrimitiveArray<u32>;
/// A type definition [`PrimitiveArray`] for `u64`
pub type UInt64Array = PrimitiveArray<u64>;

/// A type definition [`MutablePrimitiveArray`] for `i8`
pub type Int8Vec = MutablePrimitiveArray<i8>;
/// A type definition [`MutablePrimitiveArray`] for `i16`
pub type Int16Vec = MutablePrimitiveArray<i16>;
/// A type definition [`MutablePrimitiveArray`] for `i32`
pub type Int32Vec = MutablePrimitiveArray<i32>;
/// A type definition [`MutablePrimitiveArray`] for `i64`
pub type Int64Vec = MutablePrimitiveArray<i64>;
/// A type definition [`MutablePrimitiveArray`] for `i128`
pub type Int128Vec = MutablePrimitiveArray<i128>;
/// A type definition [`MutablePrimitiveArray`] for [`days_ms`]
pub type DaysMsVec = MutablePrimitiveArray<days_ms>;
/// A type definition [`MutablePrimitiveArray`] for [`months_days_ns`]
pub type MonthsDaysNsVec = MutablePrimitiveArray<months_days_ns>;
/// A type definition [`MutablePrimitiveArray`] for `f32`
pub type Float32Vec = MutablePrimitiveArray<f32>;
/// A type definition [`MutablePrimitiveArray`] for `f64`
pub type Float64Vec = MutablePrimitiveArray<f64>;
/// A type definition [`MutablePrimitiveArray`] for `u8`
pub type UInt8Vec = MutablePrimitiveArray<u8>;
/// A type definition [`MutablePrimitiveArray`] for `u16`
pub type UInt16Vec = MutablePrimitiveArray<u16>;
/// A type definition [`MutablePrimitiveArray`] for `u32`
pub type UInt32Vec = MutablePrimitiveArray<u32>;
/// A type definition [`MutablePrimitiveArray`] for `u64`
pub type UInt64Vec = MutablePrimitiveArray<u64>;
