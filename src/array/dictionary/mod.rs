use std::sync::Arc;

use crate::{
    bitmap::Bitmap,
    datatypes::DataType,
    scalar::{new_scalar, Scalar},
    types::{NativeType, NaturalDataType},
};

mod ffi;
mod iterator;
mod mutable;
pub use iterator::*;
pub use mutable::*;

use super::{new_empty_array, primitive::PrimitiveArray, Array};

/// Trait denoting [`NativeType`]s that can be used as keys of a dictionary.
pub trait DictionaryKey:
    NativeType + NaturalDataType + num_traits::NumCast + num_traits::FromPrimitive
{
}

impl DictionaryKey for i8 {}
impl DictionaryKey for i16 {}
impl DictionaryKey for i32 {}
impl DictionaryKey for i64 {}
impl DictionaryKey for u8 {}
impl DictionaryKey for u16 {}
impl DictionaryKey for u32 {}
impl DictionaryKey for u64 {}

/// An [`Array`] whose values are encoded by keys. This [`Array`] is useful when the cardinality of
/// values is low compared to the length of the [`Array`].
#[derive(Debug, Clone)]
pub struct DictionaryArray<K: DictionaryKey> {
    data_type: DataType,
    keys: PrimitiveArray<K>,
    values: Arc<dyn Array>,
    offset: usize,
}

impl<K: DictionaryKey> DictionaryArray<K> {
    /// Returns a new empty [`DictionaryArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        let values = Self::get_child(&data_type);
        let values = new_empty_array(values.clone()).into();
        Self::from_data(PrimitiveArray::<K>::new_empty(K::DATA_TYPE), values)
    }

    /// Returns an [`DictionaryArray`] whose all elements are null
    #[inline]
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        let values = Self::get_child(&data_type);
        Self::from_data(
            PrimitiveArray::<K>::new_null(K::DATA_TYPE, length),
            new_empty_array(values.clone()).into(),
        )
    }

    /// The canonical method to create a new [`DictionaryArray`].
    pub fn from_data(keys: PrimitiveArray<K>, values: Arc<dyn Array>) -> Self {
        let data_type = DataType::Dictionary(
            Box::new(keys.data_type().clone()),
            Box::new(values.data_type().clone()),
        );

        Self {
            data_type,
            keys,
            values,
            offset: 0,
        }
    }

    /// Creates a new [`DictionaryArray`] by slicing the existing [`DictionaryArray`].
    /// # Panics
    /// iff `offset + length > self.len()`.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            keys: self.keys.clone().slice(offset, length),
            values: self.values.clone(),
            offset: self.offset + offset,
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
            offset: self.offset + offset,
        }
    }

    /// Sets the validity bitmap on this [`Array`].
    /// # Panic
    /// This function panics iff `validity.len() != self.len()`.
    pub fn with_validity(&self, validity: Option<Bitmap>) -> Self {
        if matches!(&validity, Some(bitmap) if bitmap.len() != self.len()) {
            panic!("validity should be as least as large as the array")
        }
        let mut arr = self.clone();
        arr.values = Arc::from(arr.values.with_validity(validity));
        arr
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

    /// Returns the values of the [`DictionaryArray`].
    #[inline]
    pub fn values(&self) -> &Arc<dyn Array> {
        &self.values
    }

    /// Returns the value of the [`DictionaryArray`] at position `i`.
    #[inline]
    pub fn value(&self, index: usize) -> Box<dyn Scalar> {
        let index = self.keys.value(index).to_usize().unwrap();
        new_scalar(self.values.as_ref(), index)
    }
}

impl<K: DictionaryKey> DictionaryArray<K> {
    pub(crate) fn get_child(data_type: &DataType) -> &DataType {
        match data_type {
            DataType::Dictionary(_, values) => values.as_ref(),
            DataType::Extension(_, inner, _) => Self::get_child(inner),
            _ => panic!("DictionaryArray must be initialized with DataType::Dictionary"),
        }
    }
}

impl<K: DictionaryKey> Array for DictionaryArray<K> {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.keys.len()
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
        Box::new(self.with_validity(validity))
    }
}

impl<K: DictionaryKey> std::fmt::Display for DictionaryArray<K>
where
    PrimitiveArray<K>: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{:?}{{", self.data_type())?;
        writeln!(f, "keys: {},", self.keys())?;
        writeln!(f, "values: {},", self.values())?;
        write!(f, "}}")
    }
}
