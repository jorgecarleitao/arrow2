use std::sync::Arc;

use crate::{
    array::{Array, MutableArray},
    bitmap::MutableBitmap,
    datatypes::DataType,
};

use super::StructArray;

/// Converting a [`MutableStructArray`] into a [`StructArray`] is `O(1)`.
#[derive(Debug)]
pub struct MutableStructArray {
    data_type: DataType,
    values: Vec<Box<dyn MutableArray>>,
    validity: Option<MutableBitmap>,
}

impl From<MutableStructArray> for StructArray {
    fn from(other: MutableStructArray) -> Self {
        let validity = if other.validity.as_ref().map(|x| x.unset_bits()).unwrap_or(0) > 0 {
            other.validity.map(|x| x.into())
        } else {
            None
        };

        StructArray::from_data(
            other.data_type,
            other.values.into_iter().map(|mut v| v.as_box()).collect(),
            validity,
        )
    }
}

impl MutableStructArray {
    /// Creates a new [`MutableStructArray`].
    pub fn new(data_type: DataType, values: Vec<Box<dyn MutableArray>>) -> Self {
        Self::from_data(data_type, values, None)
    }

    /// Create a [`MutableStructArray`] out of low-end APIs.
    /// # Panics
    /// This function panics iff:
    /// * `data_type` is not [`DataType::Struct`]
    /// * The inner types of `data_type` are not equal to those of `values`
    /// * `validity` is not `None` and its length is different from the `values`'s length
    pub fn from_data(
        data_type: DataType,
        values: Vec<Box<dyn MutableArray>>,
        validity: Option<MutableBitmap>,
    ) -> Self {
        match data_type.to_logical_type() {
            DataType::Struct(ref fields) => assert!(fields
                .iter()
                .map(|f| f.data_type())
                .eq(values.iter().map(|f| f.data_type()))),
            _ => panic!("StructArray must be initialized with DataType::Struct"),
        };
        let self_ = Self {
            data_type,
            values,
            validity,
        };
        self_.assert_lengths();
        self_
    }

    fn assert_lengths(&self) {
        let first_len = self.values.first().map(|v| v.len());
        if let Some(len) = first_len {
            if !self.values.iter().all(|x| x.len() == len) {
                let lengths: Vec<_> = self.values.iter().map(|v| v.len()).collect();
                panic!("StructArray child lengths differ: {:?}", lengths);
            }
        }
        if let Some(validity) = &self.validity {
            assert_eq!(first_len.unwrap_or(0), validity.len());
        }
    }

    /// Extract the low-end APIs from the [`MutableStructArray`].
    pub fn into_data(self) -> (DataType, Vec<Box<dyn MutableArray>>, Option<MutableBitmap>) {
        (self.data_type, self.values, self.validity)
    }

    /// The mutable values
    pub fn mut_values(&mut self) -> &mut Vec<Box<dyn MutableArray>> {
        &mut self.values
    }

    /// The values
    pub fn values(&self) -> &Vec<Box<dyn MutableArray>> {
        &self.values
    }

    /// Return the `i`th child array.
    pub fn value<A: MutableArray + 'static>(&mut self, i: usize) -> Option<&mut A> {
        self.values[i].as_mut_any().downcast_mut::<A>()
    }
}

impl MutableStructArray {
    /// Reserves `additional` entries.
    pub fn reserve(&mut self, additional: usize) {
        for v in &mut self.values {
            v.reserve(additional);
        }
        if let Some(x) = self.validity.as_mut() {
            x.reserve(additional)
        }
    }

    /// Call this once for each "row" of children you push.
    pub fn push(&mut self, valid: bool) {
        match &mut self.validity {
            Some(validity) => validity.push(valid),
            None => match valid {
                true => (),
                false => self.init_validity(),
            },
        };
    }

    fn push_null(&mut self) {
        for v in &mut self.values {
            v.push_null();
        }
        self.push(false);
    }

    fn init_validity(&mut self) {
        let mut validity = MutableBitmap::with_capacity(self.values.capacity());
        let len = self.len();
        if len > 0 {
            validity.extend_constant(len, true);
            validity.set(len - 1, false);
        }
        self.validity = Some(validity)
    }

    /// Converts itself into an [`Array`].
    pub fn into_arc(self) -> Arc<dyn Array> {
        let a: StructArray = self.into();
        Arc::new(a)
    }

    /// Shrinks the capacity of the [`MutableStructArray`] to fit its current length.
    pub fn shrink_to_fit(&mut self) {
        for v in &mut self.values {
            v.shrink_to_fit();
        }
        if let Some(validity) = self.validity.as_mut() {
            validity.shrink_to_fit()
        }
    }
}

impl MutableArray for MutableStructArray {
    fn len(&self) -> usize {
        self.values.first().map(|v| v.len()).unwrap_or(0)
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        self.validity.as_ref()
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(StructArray::from_data(
            self.data_type.clone(),
            std::mem::take(&mut self.values)
                .into_iter()
                .map(|mut v| v.as_box())
                .collect(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        ))
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(StructArray::from_data(
            self.data_type.clone(),
            std::mem::take(&mut self.values)
                .into_iter()
                .map(|mut v| v.as_box())
                .collect(),
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
        self.push_null()
    }

    fn shrink_to_fit(&mut self) {
        self.shrink_to_fit()
    }

    fn reserve(&mut self, additional: usize) {
        self.reserve(additional)
    }
}
