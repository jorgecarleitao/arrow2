use std::sync::Arc;

use crate::{
    array::{Array, MutableArray, MutableStructArray, StructArray},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::{Error, Result},
    types::Index,
};

use super::MapArray;

/// The mutable version lf [`MapArray`].
#[derive(Debug)]
pub struct MutableMapArray {
    data_type: DataType,
    offsets: Vec<i32>,
    field: MutableStructArray,
    validity: Option<MutableBitmap>,
}

impl From<MutableMapArray> for MapArray {
    fn from(other: MutableMapArray) -> Self {
        let validity = if other.validity.as_ref().map(|x| x.unset_bits()).unwrap_or(0) > 0 {
            other.validity.map(|x| x.into())
        } else {
            None
        };

        let field: StructArray = other.field.into();

        MapArray::from_data(
            other.data_type,
            other.offsets.into(),
            Box::new(field),
            validity,
        )
    }
}

impl MutableMapArray {
    /// Creates a new empty [`MutableMapArray`].
    /// # Errors
    /// This function errors if:
    /// * The `data_type`'s physical type is not [`crate::datatypes::PhysicalType::Map`]
    /// * The fields' `data_type` is not equal to the inner field of `data_type`
    pub fn try_new(data_type: DataType, values: Vec<Box<dyn MutableArray>>) -> Result<Self> {
        let field = MutableStructArray::try_from_data(
            MapArray::get_field(&data_type).data_type().clone(),
            values,
            None,
        )?;
        Ok(Self {
            data_type,
            offsets: vec![0i32; 1],
            field,
            validity: None,
        })
    }

    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    /// Mutable reference to the field
    pub fn mut_field(&mut self) -> &mut MutableStructArray {
        &mut self.field
    }

    /// Reference to the field
    pub fn field(&self) -> &MutableStructArray {
        &self.field
    }

    /// Get a mutable reference to the keys array
    pub fn keys<A: MutableArray + 'static>(&mut self) -> &mut A {
        self.field.value(0).unwrap()
    }

    /// Get a mutable reference to the values array
    pub fn values<A: MutableArray + 'static>(&mut self) -> &mut A {
        self.field.value(1).unwrap()
    }

    /// Call this once for each "row" of children you push.
    pub fn push(&mut self, valid: bool) {
        match &mut self.validity {
            Some(validity) => validity.push(valid),
            None => match valid {
                true => (),
                false => self.init_validity(),
            },
        }
    }

    #[inline]
    /// Needs to be called when a valid value was extended to this array.
    pub fn try_push_valid(&mut self) -> Result<()> {
        let size = self.field.len();
        let size = <i32 as Index>::from_usize(size).ok_or(Error::Overflow)?;
        assert!(size >= *self.offsets.last().unwrap());
        self.offsets.push(size);
        if let Some(validity) = &mut self.validity {
            validity.push(true)
        }
        Ok(())
    }

    fn push_null(&mut self) {
        self.field.push(false);
        self.push(false);
    }

    fn init_validity(&mut self) {
        let mut validity = MutableBitmap::with_capacity(self.field.len());
        let len = self.len();
        if len > 0 {
            validity.extend_constant(len, true);
            validity.set(len - 1, false);
        }
        self.validity = Some(validity);
    }

    fn take_into(&mut self) -> MapArray {
        MapArray::from_data(
            self.data_type.clone(),
            std::mem::take(&mut self.offsets).into(),
            self.field.as_box(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        )
    }
}

impl MutableArray for MutableMapArray {
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        self.validity.as_ref()
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(self.take_into())
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(self.take_into())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn push_null(&mut self) {
        self.push_null()
    }

    fn reserve(&mut self, additional: usize) {
        self.offsets.reserve(additional);
        self.field.reserve(additional);
        if let Some(validity) = &mut self.validity {
            validity.reserve(additional)
        }
    }

    fn shrink_to_fit(&mut self) {
        self.offsets.shrink_to_fit();
        self.field.shrink_to_fit();
        if let Some(validity) = &mut self.validity {
            validity.shrink_to_fit();
        }
    }
}
