use std::sync::Arc;

use crate::{
    bitmap::Bitmap,
    datatypes::{DataType, Field},
    error::Result,
    ffi,
};

use super::{ffi::ToFfi, new_empty_array, new_null_array, Array, FromFfi};

#[derive(Debug, Clone)]
pub struct StructArray {
    data_type: DataType,
    values: Vec<Arc<dyn Array>>,
    validity: Option<Bitmap>,
}

impl StructArray {
    pub fn new_empty(fields: &[Field]) -> Self {
        let values = fields
            .iter()
            .map(|field| new_empty_array(field.data_type().clone()).into())
            .collect();
        Self::from_data(fields.to_vec(), values, None)
    }

    #[inline]
    pub fn new_null(fields: &[Field], length: usize) -> Self {
        let values = fields
            .iter()
            .map(|field| new_null_array(field.data_type().clone(), length).into())
            .collect();
        Self::from_data(fields.to_vec(), values, Some(Bitmap::new_zeroed(length)))
    }

    pub fn from_data(
        fields: Vec<Field>,
        values: Vec<Arc<dyn Array>>,
        validity: Option<Bitmap>,
    ) -> Self {
        assert!(!fields.is_empty());
        assert_eq!(fields.len(), values.len());
        assert!(values.iter().all(|x| x.len() == values[0].len()));
        if let Some(ref validity) = validity {
            assert_eq!(values[0].len(), validity.len());
        }
        Self {
            data_type: DataType::Struct(fields),
            values,
            validity,
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.clone().map(|x| x.slice(offset, length));
        Self {
            data_type: self.data_type.clone(),
            values: self
                .values
                .iter()
                .map(|x| x.slice(offset, length).into())
                .collect(),
            validity,
        }
    }

    #[inline]
    pub fn values(&self) -> &[Arc<dyn Array>] {
        &self.values
    }

    #[inline]
    pub fn fields(&self) -> &[Field] {
        Self::get_fields(&self.data_type)
    }
}

impl StructArray {
    pub fn get_fields(datatype: &DataType) -> &[Field] {
        if let DataType::Struct(fields) = datatype {
            fields
        } else {
            panic!("Wrong datatype passed to Struct.")
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
        self.values[0].len()
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    #[inline]
    fn validity(&self) -> &Option<Bitmap> {
        &self.validity
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
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

unsafe impl ToFfi for StructArray {
    fn buffers(&self) -> Vec<Option<std::ptr::NonNull<u8>>> {
        vec![self.validity.as_ref().map(|x| x.as_ptr())]
    }

    fn offset(&self) -> usize {
        // we do not support offsets in structs. Instead, if an FFI we slice the incoming arrays
        0
    }

    fn children(&self) -> Vec<Arc<dyn Array>> {
        self.values.clone()
    }
}

unsafe impl<A: ffi::ArrowArrayRef> FromFfi<A> for StructArray {
    fn try_from_ffi(array: A) -> Result<Self> {
        let data_type = array.data_type()?;
        let fields = Self::get_fields(&data_type).to_vec();

        let length = array.array().len();
        let offset = array.array().offset();
        let mut validity = unsafe { array.validity() }?;
        let values = (0..fields.len())
            .map(|index| {
                let child = array.child(index)?;
                Ok(ffi::try_from(child)?.into())
            })
            .collect::<Result<Vec<Arc<dyn Array>>>>()?;

        if offset > 0 {
            validity = validity.map(|x| x.slice(offset, length))
        }
        Ok(Self::from_data(fields, values, validity))
    }
}
