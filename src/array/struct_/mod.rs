use std::{ops::Index, sync::Arc};

use crate::{
    bitmap::Bitmap,
    datatypes::{DataType, Field},
};

use super::{new_empty_array, new_null_array, Array};

mod ffi;
mod iterator;

/// A [`StructArray`] is a nested [`Array`] with an optional validity representing
/// multiple [`Array`] with the same number of rows.
/// # Example
/// ```
/// use std::sync::Arc;
/// use arrow2::array::*;
/// use arrow2::datatypes::*;
/// let boolean = Arc::new(BooleanArray::from_slice(&[false, false, true, true])) as Arc<dyn Array>;
/// let int = Arc::new(Int32Array::from_slice(&[42, 28, 19, 31])) as Arc<dyn Array>;
///
/// let fields = vec![
///     Field::new("b", DataType::Boolean, false),
///     Field::new("c", DataType::Int32, false),
/// ];
///
/// let array = StructArray::from_data(DataType::Struct(fields), vec![boolean, int], None);
/// ```
#[derive(Debug, Clone)]
pub struct StructArray {
    data_type: DataType,
    values: Vec<Arc<dyn Array>>,
    validity: Option<Bitmap>,
}

impl StructArray {
    /// Creates an empty [`StructArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        if let DataType::Struct(fields) = &data_type {
            let values = fields
                .iter()
                .map(|field| new_empty_array(field.data_type().clone()).into())
                .collect();
            Self::from_data(data_type, values, None)
        } else {
            panic!("StructArray must be initialized with DataType::Struct");
        }
    }

    /// Creates a null [`StructArray`] of length `length`.
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        if let DataType::Struct(fields) = &data_type {
            let values = fields
                .iter()
                .map(|field| new_null_array(field.data_type().clone(), length).into())
                .collect();
            Self::from_data(data_type, values, Some(Bitmap::new_zeroed(length)))
        } else {
            panic!("StructArray must be initialized with DataType::Struct");
        }
    }

    /// Canonical method to create a [`StructArray`].
    /// # Panics
    /// * fields are empty
    /// * values's len is different from Fields' length.
    /// * any element of values has a different length than the first element.
    pub fn from_data(
        data_type: DataType,
        values: Vec<Arc<dyn Array>>,
        validity: Option<Bitmap>,
    ) -> Self {
        let fields = Self::get_fields(&data_type);
        assert!(!fields.is_empty());
        assert_eq!(fields.len(), values.len());
        assert!(values.iter().all(|x| x.len() == values[0].len()));
        if let Some(ref validity) = validity {
            assert_eq!(values[0].len(), validity.len());
        }
        Self {
            data_type,
            values,
            validity,
        }
    }

    /// Deconstructs the [`StructArray`] into its individual components.
    pub fn into_data(self) -> (Vec<Field>, Vec<Arc<dyn Array>>, Option<Bitmap>) {
        let Self {
            data_type,
            values,
            validity,
        } = self;
        let fields = if let DataType::Struct(fields) = data_type {
            fields
        } else {
            unreachable!()
        };
        (fields, values, validity)
    }

    /// Creates a new [`StructArray`] that is a slice of `self`.
    /// # Panics
    /// * `offset + length` must be smaller than `self.len()`.
    /// # Implementation
    /// This operation is `O(F)` where `F` is the number of fields.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Creates a new [`StructArray`] that is a slice of `self`.
    /// # Implementation
    /// This operation is `O(F)` where `F` is the number of fields.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    pub unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        let validity = self
            .validity
            .clone()
            .map(|x| x.slice_unchecked(offset, length));
        Self {
            data_type: self.data_type.clone(),
            values: self
                .values
                .iter()
                .map(|x| x.slice_unchecked(offset, length).into())
                .collect(),
            validity,
        }
    }

    /// Sets the validity bitmap on this [`StructArray`].
    /// # Panic
    /// This function panics iff `validity.len() != self.len()`.
    pub fn with_validity(&self, validity: Option<Bitmap>) -> Self {
        if matches!(&validity, Some(bitmap) if bitmap.len() != self.len()) {
            panic!("validity should be as least as large as the array")
        }
        let mut arr = self.clone();
        arr.validity = validity;
        arr
    }
}

// Accessors
impl StructArray {
    #[inline]
    fn len(&self) -> usize {
        self.values[0].len()
    }

    /// The optional validity.
    #[inline]
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// Returns the values of this [`StructArray`].
    pub fn values(&self) -> &[Arc<dyn Array>] {
        &self.values
    }

    /// Returns the fields of this [`StructArray`].
    pub fn fields(&self) -> &[Field] {
        Self::get_fields(&self.data_type)
    }

    /// Returns the element at index `i`
    pub fn value(&self, index: usize) -> &Arc<dyn Array> {
        self.values().index(index)
    }
}

impl StructArray {
    /// Returns the fields the `DataType::Struct`.
    pub fn get_fields(data_type: &DataType) -> &[Field] {
        match data_type {
            DataType::Struct(fields) => fields,
            DataType::Extension(_, inner, _) => Self::get_fields(inner),
            _ => panic!("Wrong datatype passed to Struct."),
        }
    }
}

impl Array for StructArray {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
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

    #[inline]
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
}

impl std::fmt::Display for StructArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "StructArray{{")?;
        for (field, column) in self.fields().iter().zip(self.values()) {
            writeln!(f, "{}: {},", field.name(), column)?;
        }
        write!(f, "}}")
    }
}
