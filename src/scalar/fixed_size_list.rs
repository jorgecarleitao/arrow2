use std::any::Any;
use std::sync::Arc;

use crate::{array::*, datatypes::DataType};

use super::Scalar;

/// The scalar equivalent of [`FixedSizeListArray`]. Like [`FixedSizeListArray`], this struct holds a dynamically-typed
/// [`Array`]. The only difference is that this has only one element.
#[derive(Debug, Clone)]
pub struct FixedSizeListScalar {
    values: Arc<dyn Array>,
    is_valid: bool,
    data_type: DataType,
}

impl PartialEq for FixedSizeListScalar {
    fn eq(&self, other: &Self) -> bool {
        (self.data_type == other.data_type)
            && (self.is_valid == other.is_valid)
            && ((!self.is_valid) | (self.values.as_ref() == other.values.as_ref()))
    }
}

impl FixedSizeListScalar {
    /// returns a new [`FixedSizeListScalar`]
    /// # Panics
    /// iff
    /// * the `data_type` is not `FixedSizeList`
    /// * the child of the `data_type` is not equal to the `values`
    /// * the size of child array is not equal
    #[inline]
    pub fn new(data_type: DataType, values: Option<Arc<dyn Array>>) -> Self {
        let (field, size) = FixedSizeListArray::get_child_and_size(&data_type);
        let inner_data_type = field.data_type();
        let (is_valid, values) = match values {
            Some(values) => {
                assert_eq!(inner_data_type, values.data_type());
                assert_eq!(size, values.len());
                (true, values)
            }
            None => (false, new_empty_array(inner_data_type.clone()).into()),
        };
        Self {
            values,
            is_valid,
            data_type,
        }
    }

    /// The values of the [`FixedSizeListScalar`]
    pub fn values(&self) -> &Arc<dyn Array> {
        &self.values
    }
}

impl Scalar for FixedSizeListScalar {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}
