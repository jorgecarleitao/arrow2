use std::hint::unreachable_unchecked;

use crate::{
    bitmap::{
        utils::{zip_validity, ZipValidity},
        Bitmap,
    },
    datatypes::{DataType, IntegerType},
    error::Error,
    scalar::{new_scalar, Scalar},
    trusted_len::TrustedLen,
    types::NativeType,
};

mod ffi;
pub(super) mod fmt;
mod iterator;
mod mutable;
pub use iterator::*;
pub use mutable::*;

use super::{new_empty_array, primitive::PrimitiveArray, Array};
use super::{new_null_array, specification::check_indexes};

/// Trait denoting [`NativeType`]s that can be used as keys of a dictionary.
pub trait DictionaryKey: NativeType + TryInto<usize> + TryFrom<usize> {
    /// The corresponding [`IntegerType`] of this key
    const KEY_TYPE: IntegerType;

    /// Represents this key as a `usize`.
    /// # Safety
    /// The caller _must_ have checked that the value can be casted to `usize`.
    #[inline]
    unsafe fn as_usize(self) -> usize {
        match self.try_into() {
            Ok(v) => v,
            Err(_) => unreachable_unchecked(),
        }
    }
}

impl DictionaryKey for i8 {
    const KEY_TYPE: IntegerType = IntegerType::Int8;
}
impl DictionaryKey for i16 {
    const KEY_TYPE: IntegerType = IntegerType::Int16;
}
impl DictionaryKey for i32 {
    const KEY_TYPE: IntegerType = IntegerType::Int32;
}
impl DictionaryKey for i64 {
    const KEY_TYPE: IntegerType = IntegerType::Int64;
}
impl DictionaryKey for u8 {
    const KEY_TYPE: IntegerType = IntegerType::UInt8;
}
impl DictionaryKey for u16 {
    const KEY_TYPE: IntegerType = IntegerType::UInt16;
}
impl DictionaryKey for u32 {
    const KEY_TYPE: IntegerType = IntegerType::UInt32;
}
impl DictionaryKey for u64 {
    const KEY_TYPE: IntegerType = IntegerType::UInt64;
}

/// An [`Array`] whose values are stored as indices. This [`Array`] is useful when the cardinality of
/// values is low compared to the length of the [`Array`].
///
/// # Safety
/// This struct guarantees that each item of [`DictionaryArray::keys`] is castable to `usize` and
/// its value is smaller than [`DictionaryArray::values`]`.len()`. In other words, you can safely
/// use `unchecked` calls to retrive the values
#[derive(Clone)]
pub struct DictionaryArray<K: DictionaryKey> {
    data_type: DataType,
    keys: PrimitiveArray<K>,
    values: Box<dyn Array>,
}

fn check_data_type(
    key_type: IntegerType,
    data_type: &DataType,
    values_data_type: &DataType,
) -> Result<(), Error> {
    if let DataType::Dictionary(key, value, _) = data_type.to_logical_type() {
        if *key != key_type {
            return Err(Error::oos(
                "DictionaryArray must be initialized with a DataType::Dictionary whose integer is compatible to its keys",
            ));
        }
        if value.as_ref().to_logical_type() != values_data_type.to_logical_type() {
            return Err(Error::oos(
                "DictionaryArray must be initialized with a DataType::Dictionary whose value is equal to its values",
            ));
        }
    } else {
        return Err(Error::oos(
            "DictionaryArray must be initialized with logical DataType::Dictionary",
        ));
    }
    Ok(())
}

impl<K: DictionaryKey> DictionaryArray<K> {
    /// Returns a new [`DictionaryArray`].
    /// # Implementation
    /// This function is `O(N)` where `N` is the length of keys
    /// # Errors
    /// This function errors iff
    /// * the `data_type`'s logical type is not a `DictionaryArray`
    /// * the `data_type`'s keys is not compatible with `keys`
    /// * the `data_type`'s values's data_type is not equal with `values.data_type()`
    /// * any of the keys's values is not represented in `usize` or is `>= values.len()`
    pub fn try_new(
        data_type: DataType,
        keys: PrimitiveArray<K>,
        values: Box<dyn Array>,
    ) -> Result<Self, Error> {
        check_data_type(K::KEY_TYPE, &data_type, values.data_type())?;

        check_indexes(keys.iter(), values.len())?;

        Ok(Self {
            data_type,
            keys,
            values,
        })
    }

    /// Returns a new [`DictionaryArray`].
    /// # Implementation
    /// This function is `O(N)` where `N` is the length of keys
    /// # Errors
    /// This function errors iff
    /// * any of the keys's values is not represented in `usize` or is `>= values.len()`
    pub fn try_from_keys(keys: PrimitiveArray<K>, values: Box<dyn Array>) -> Result<Self, Error> {
        let data_type = Self::default_data_type(values.data_type().clone());
        Self::try_new(data_type, keys, values)
    }

    /// Returns a new [`DictionaryArray`].
    /// # Errors
    /// This function errors iff
    /// * the `data_type`'s logical type is not a `DictionaryArray`
    /// * the `data_type`'s keys is not compatible with `keys`
    /// * the `data_type`'s values's data_type is not equal with `values.data_type()`
    /// # Safety
    /// The caller must ensure that every keys's values is represented in `usize` and is `< values.len()`
    pub unsafe fn try_new_unchecked(
        data_type: DataType,
        keys: PrimitiveArray<K>,
        values: Box<dyn Array>,
    ) -> Result<Self, Error> {
        check_data_type(K::KEY_TYPE, &data_type, values.data_type())?;

        Ok(Self {
            data_type,
            keys,
            values,
        })
    }

    /// Returns a new empty [`DictionaryArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        let values = Self::try_get_child(&data_type).unwrap();
        let values = new_empty_array(values.clone());
        Self::try_new(
            data_type,
            PrimitiveArray::<K>::new_empty(K::PRIMITIVE.into()),
            values,
        )
        .unwrap()
    }

    /// Returns an [`DictionaryArray`] whose all elements are null
    #[inline]
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        let values = Self::try_get_child(&data_type).unwrap();
        let values = new_null_array(values.clone(), 1);
        Self::try_new(
            data_type,
            PrimitiveArray::<K>::new_null(K::PRIMITIVE.into(), length),
            values,
        )
        .unwrap()
    }

    /// Returns an iterator of [`Option<Box<dyn Scalar>>`].
    /// # Implementation
    /// This function will allocate a new [`Scalar`] per item and is usually not performant.
    /// Consider calling `keys_iter` and `values`, downcasting `values`, and iterating over that.
    pub fn iter(&self) -> ZipValidity<Box<dyn Scalar>, DictionaryValuesIter<K>> {
        zip_validity(
            DictionaryValuesIter::new(self),
            self.keys.validity().as_ref().map(|x| x.iter()),
        )
    }

    /// Returns an iterator of [`Box<dyn Scalar>`]
    /// # Implementation
    /// This function will allocate a new [`Scalar`] per item and is usually not performant.
    /// Consider calling `keys_iter` and `values`, downcasting `values`, and iterating over that.
    pub fn values_iter(&self) -> DictionaryValuesIter<K> {
        DictionaryValuesIter::new(self)
    }

    /// Returns the [`DataType`] of this [`DictionaryArray`]
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns whether the values of this [`DictionaryArray`] are ordered
    #[inline]
    pub fn is_ordered(&self) -> bool {
        match self.data_type.to_logical_type() {
            DataType::Dictionary(_, _, is_ordered) => *is_ordered,
            _ => unreachable!(),
        }
    }

    pub(crate) fn default_data_type(values_datatype: DataType) -> DataType {
        DataType::Dictionary(K::KEY_TYPE, Box::new(values_datatype), false)
    }

    /// Creates a new [`DictionaryArray`] by slicing the existing [`DictionaryArray`].
    /// # Panics
    /// iff `offset + length > self.len()`.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            keys: self.keys.clone().slice(offset, length),
            values: self.values.clone(),
        }
    }

    /// Creates a new [`DictionaryArray`] by slicing the existing [`DictionaryArray`].
    /// # Safety
    /// Safe iff `offset + length <= self.len()`.
    pub unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            keys: self.keys.clone().slice_unchecked(offset, length),
            values: self.values.clone(),
        }
    }

    /// Returns this [`DictionaryArray`] with a new validity.
    /// # Panic
    /// This function panics iff `validity.len() != self.len()`.
    #[must_use]
    pub fn with_validity(mut self, validity: Option<Bitmap>) -> Self {
        self.set_validity(validity);
        self
    }

    /// Sets the validity of the keys of this [`DictionaryArray`].
    /// # Panics
    /// This function panics iff `validity.len() != self.len()`.
    pub fn set_validity(&mut self, validity: Option<Bitmap>) {
        self.keys.set_validity(validity);
    }

    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// The optional validity. Equivalent to `self.keys().validity()`.
    #[inline]
    pub fn validity(&self) -> Option<&Bitmap> {
        self.keys.validity()
    }

    /// Returns the keys of the [`DictionaryArray`]. These keys can be used to fetch values
    /// from `values`.
    #[inline]
    pub fn keys(&self) -> &PrimitiveArray<K> {
        &self.keys
    }

    /// Returns an iterator of the keys' values of the [`DictionaryArray`] as `usize`
    #[inline]
    pub fn keys_values_iter(&self) -> impl TrustedLen<Item = usize> + Clone + '_ {
        // safety - invariant of the struct
        self.keys.values_iter().map(|x| unsafe { x.as_usize() })
    }

    /// Returns an iterator of the keys' of the [`DictionaryArray`] as `usize`
    #[inline]
    pub fn keys_iter(&self) -> impl TrustedLen<Item = Option<usize>> + Clone + '_ {
        // safety - invariant of the struct
        self.keys.iter().map(|x| x.map(|x| unsafe { x.as_usize() }))
    }

    /// Returns the keys' value of the [`DictionaryArray`] as `usize`
    /// # Panics
    /// This function panics iff `index >= self.len()`
    #[inline]
    pub fn key_value(&self, index: usize) -> usize {
        // safety - invariant of the struct
        unsafe { self.keys.values()[index].as_usize() }
    }

    /// Returns the values of the [`DictionaryArray`].
    #[inline]
    pub fn values(&self) -> &Box<dyn Array> {
        &self.values
    }

    /// Returns the value of the [`DictionaryArray`] at position `i`.
    /// # Implementation
    /// This function will allocate a new [`Scalar`] and is usually not performant.
    /// Consider calling `keys` and `values`, downcasting `values`, and iterating over that.
    /// # Panic
    /// This function panics iff `index >= self.len()`
    #[inline]
    pub fn value(&self, index: usize) -> Box<dyn Scalar> {
        // safety - invariant of this struct
        let index = unsafe { self.keys.value(index).as_usize() };
        new_scalar(self.values.as_ref(), index)
    }

    /// Boxes self into a [`Box<dyn Array>`].
    pub fn boxed(self) -> Box<dyn Array> {
        Box::new(self)
    }

    /// Boxes self into a [`std::sync::Arc<dyn Array>`].
    pub fn arced(self) -> std::sync::Arc<dyn Array> {
        std::sync::Arc::new(self)
    }

    pub(crate) fn try_get_child(data_type: &DataType) -> Result<&DataType, Error> {
        Ok(match data_type.to_logical_type() {
            DataType::Dictionary(_, values, _) => values.as_ref(),
            _ => {
                return Err(Error::oos(
                    "Dictionaries must be initialized with DataType::Dictionary",
                ))
            }
        })
    }
}

impl<K: DictionaryKey> Array for DictionaryArray<K> {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn validity(&self) -> Option<&Bitmap> {
        self.keys.validity()
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }

    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice_unchecked(offset, length))
    }

    fn with_validity(&self, validity: Option<Bitmap>) -> Box<dyn Array> {
        Box::new(self.clone().with_validity(validity))
    }

    fn to_boxed(&self) -> Box<dyn Array> {
        Box::new(self.clone())
    }
}
